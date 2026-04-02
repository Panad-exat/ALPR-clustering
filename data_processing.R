# ==============================================================================
# 📄 SCRIPT NAME: data_preprocessing_validation.R
# 🎯 PURPOSE: Traffic Big Data Preprocessing, Data Validation, and Monthly Aggregation
# 🛠️ SYSTEM: Automatic License Plate Recognition (ALPR) - Chalong Rat Expressway
# ==============================================================================
# ------------------------------------------------------------------------------
# BLOCK 1: SETUP & LIBRARIES
# ------------------------------------------------------------------------------
print("⏳ Installing dependencies...")
system("apt-get update -qq", ignore.stdout = TRUE, ignore.stderr = TRUE)
system("apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev", ignore.stdout = TRUE, ignore.stderr = TRUE)

pkgs <- c("googledrive", "data.table", "lubridate", "stringr")
new_pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new_pkgs)) install.packages(new_pkgs, quiet = TRUE)

suppressPackageStartupMessages({
  library(googledrive)
  library(data.table)
  library(lubridate)
  library(stringr)
})

# --- OPTIMIZATION SETTINGS ---
setDTthreads(0)                 # ใช้ CPU เต็มสูบ
options(googledrive_quiet = TRUE) # ปิด Warning แบบถูกวิธี

print("🔑 Authentication...")
drive_auth(use_oob = TRUE)

# ------------------------------------------------------------------------------
# BLOCK 2: LOGIC FUNCTIONS (Classification)
# ------------------------------------------------------------------------------
tier3_keywords <- c("BENZ", "BMW", "VOLVO", "AUDI", "PORSCHE", "MINI", "LEXUS",
                    "TESLA", "LAND ROVER", "JAGUAR", "FERRARI", "LAMBORGHINI",
                    "MASERATI", "BENTLEY", "ROLLS", "ASTON", "MCLAREN", "LOTUS",
                    "ALFA ROMEO", "MAYBACH")

get_tier_score <- function(brand_vec, type_vec) {
  brand_clean <- str_to_upper(str_trim(brand_vec))
  type_clean <- str_to_upper(str_trim(type_vec))
  fcase(
    grepl("6|10|>10|รถบรรทุก|TRUCK|BUS|รถโดยสาร", type_clean), 2,
    grepl(paste(tier3_keywords, collapse = "|"), brand_clean), 3,
    default = 1
  )
}

bkk_vicinity_provinces <- c("กรุงเทพมหานคร", "นนทบุรี", "ปทุมธานี", "สมุทรปราการ", "สมุทรสาคร", "นครปฐม")
get_province_group <- function(province_vec) {
  prov_clean <- str_trim(province_vec)
  fcase(prov_clean %in% bkk_vicinity_provinces, 1, default = 2)
}

# ------------------------------------------------------------------------------
# BLOCK 3: FILE DISCOVERY
# ------------------------------------------------------------------------------
target_folder <- "ALPR-Q3-Q4-Data"
folder_id <- drive_find(pattern = target_folder, type = "folder")

if(nrow(folder_id) == 0) stop("❌ Error: ไม่เจอ Folder!")

files_list <- drive_ls(as_id(folder_id$id), pattern = "\\.csv$", n_max = 50000)
print(paste("✅ Ready to process:", nrow(files_list), "files"))


# ------------------------------------------------------------------------------
# BLOCK 4: FEATURE EXTRACTION 
# ------------------------------------------------------------------------------
process_features <- function(file_row) {

  temp_name <- paste0("temp_feat_", file_row$name)
  tryCatch({ drive_download(as_id(file_row$id), path = temp_name, overwrite = TRUE) },
           error = function(e) return(NULL))

  if (!file.exists(temp_name)) return(NULL)

  # Robust Read
  dt <- tryCatch({ fread(temp_name, encoding = "UTF-8") }, error = function(e) {
    tryCatch({ as.data.table(read.csv(temp_name, fileEncoding = "UTF-16LE", stringsAsFactors = FALSE)) },
             error = function(e2) {
               tryCatch({ as.data.table(read.csv(temp_name, fileEncoding = "UCS-2LE", stringsAsFactors = FALSE)) },
                        error = function(e3) return(NULL))
             })
  })

  if (is.null(dt) || nrow(dt) == 0) {
    if(file.exists(temp_name)) file.remove(temp_name)
    return(NULL)
  }

  names(dt) <- tolower(names(dt))
  raw_row_count <- nrow(dt)

  cat(paste0("\n📄 ไฟล์: ", file_row$name))
  cat(paste0("\n   -> อ่านไฟล์ได้: ", format(raw_row_count, big.mark=","), " แถว"))

  
  # 🔴 1. Cleansing ทะเบียนรถ
  col_plate <- grep("plate|ทะเบียน", names(dt), value = TRUE, ignore.case = TRUE)[1]
  if (is.na(col_plate)) { file.remove(temp_name); return(NULL) }
  setnames(dt, col_plate, "plate_number")

  dt <- dt[plate_number != "" & !is.na(plate_number)]
  dt <- dt[!grepl("^UNKNOWN|^ไม่ทราบ|^ไม่มี|^TEST|^ADMIN|^VIP", plate_number, ignore.case = TRUE)]

  if(nrow(dt) == 0) { file.remove(temp_name); return(NULL) }

  # 🔴 2. จัดการเวลาเร่งด่วน (Peak Hour) - แก้ไขปัญหาจุดทศนิยม
  col_time <- grep("entry_time|เวลาเข้า", names(dt), value = TRUE, ignore.case = TRUE)[1]
  if (!is.na(col_time)) {
    # ดึงเฉพาะ "ตัวเลขชั่วโมง" ที่อยู่หน้าจุด (.) หรือ โคลอน (:) มาเป็นตัวเลข
    dt[, hour_of_day := as.numeric(sub("[:\\.].*$", "", as.character(get(col_time))))]
    # กำหนด Peak: 6, 7, 8 โมงเช้า และ 16, 17, 18 โมงเย็น
    dt[, is_peak := fcase(hour_of_day %in% c(6, 7, 8, 16, 17, 18), 1, default = 0)]
  } else {
    dt[, is_peak := 0]
  }

  # 🔴 3. ดึงระยะเวลาเดินทาง (Travel Time) 
  col_travel <- grep("travel|นาที|เวลาเดินทาง", names(dt), value = TRUE, ignore.case = TRUE)[1]
  if (!is.na(col_travel)) {
    # คลีนข้อมูลตัวอักษรที่อาจปนมา (ถ้ามี) ให้เหลือแต่ตัวเลข
    dt[, travel_duration := as.numeric(gsub("[^0-9.]", "", as.character(get(col_travel))))]
  } else {
    dt[, travel_duration := NA_real_]
  }

  # 🔴 4. Dynamic Column Detection สำหรับตัวแปรอื่นๆ
  col_payment <- grep("payment|วิธี|ชำระ", names(dt), value = TRUE, ignore.case = TRUE)[1]
  col_brand <- grep("brand|ยี่ห้อ", names(dt), value = TRUE, ignore.case = TRUE)[1]
  col_type <- grep("type|class|ประเภท", names(dt), value = TRUE, ignore.case = TRUE)[1]
  col_prov <- grep("province|prov|จังหวัด", names(dt), value = TRUE, ignore.case = TRUE)[1]

  val_payment <- if(!is.na(col_payment)) dt[[col_payment]] else rep("CASH", nrow(dt))
  val_brand <- if(!is.na(col_brand)) dt[[col_brand]] else rep("UNKNOWN", nrow(dt))
  val_type <- if(!is.na(col_type)) dt[[col_type]] else rep("UNKNOWN", nrow(dt))
  val_prov <- if(!is.na(col_prov)) dt[[col_prov]] else rep("ไม่ระบุ", nrow(dt))

  dt[, is_etc := fifelse(grepl("ETC|EASY PASS|M-FLOW", toupper(val_payment)), 1, 0)]
  dt[, tier_score := get_tier_score(val_brand, val_type)]
  dt[, prov_group := get_province_group(val_prov)]
  dt[, actual_province := val_prov] 

  # ดึงเฉพาะคอลัมน์ที่จำเป็นออกไป
  out_dt <- dt[, .(plate_number, actual_province, is_peak, travel_duration, is_etc, tier_score, prov_group)]

  file.remove(temp_name)
  return(out_dt)
}

# --- RUNNING LOOP ---
print("\n🚀 เริ่มสกัดข้อมูลพฤติกรรม (Feature Engineering)...")
all_features <- lapply(1:nrow(files_list), function(i) {
  process_features(files_list[i,])
})

cat("\n✅ อ่านไฟล์เสร็จสิ้น กำลังรวมข้อมูล...\n")
master_dt <- rbindlist(all_features[!sapply(all_features, is.null)], fill = TRUE)

# ------------------------------------------------------------------------------
# BLOCK 5: AGGREGATION FOR CLUSTERING & GOOGLE DRIVE UPLOAD
# ------------------------------------------------------------------------------
if (nrow(master_dt) > 0) {
    cat("\n📊 กำลังสร้าง User Profiles สำหรับ SPSS...\n")

    user_profiles <- master_dt[, .(
      Freq_Total = .N,
      Freq_Per_Month = round(.N / 3, 2), # ข้อมูล 3 เดือน
      Avg_Travel_Time = if(all(is.na(travel_duration))) NA_real_ else round(mean(travel_duration[travel_duration > 0 & travel_duration < 120], na.rm = TRUE), 2),
      ETC_Rate = round((sum(is_etc, na.rm=TRUE) / .N) * 100, 2),
      Peak_Usage_Rate = round((sum(is_peak, na.rm=TRUE) / .N) * 100, 2),
      Brand_Tier = max(tier_score, na.rm = TRUE),
      Province_Group = max(prov_group, na.rm = TRUE)
    ), by = .(plate_number, actual_province)]

    # กรอง Outlier (ตัดรถวิ่งเกิน 2700 เที่ยว)
    user_profiles <- user_profiles[Freq_Total <= 2700]

    print("-------------------------------------------------------------")
    print(paste("✅ สร้างโปรไฟล์สำเร็จ จำนวน:", format(nrow(user_profiles), big.mark=","), "คัน (Unique Users)"))
    print("-------------------------------------------------------------")

    # Save ลงเครื่อง
    output_filename <- "ALPR_User_Profiles_For_SPSS.csv"
    fwrite(user_profiles, output_filename)

    # Upload ลง Google Drive
    cat("\n☁️ กำลังอัปโหลดไฟล์ผลลัพธ์กลับสู่ Google Drive...\n")
    drive_upload(
      media = output_filename,
      path = as_id(folder_id$id),
      name = "FINAL_ALPR_Profiles_SPSS.csv",
      overwrite = TRUE
    )
    cat("🎉 อัปโหลดสำเร็จ! ไฟล์ FINAL_ALPR_Profiles_SPSS.csv พร้อมใช้งานใน Drive \n")

} else {
    print("❌ ไม่พบข้อมูลที่ผ่านการกรอง")
}
