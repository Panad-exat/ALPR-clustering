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
# BLOCK 4: FEATURE EXTRACTION (Fixed Province Column)
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

  # Spot Check
  cat(paste0("\n📄 ไฟล์: ", file_row$name))
  cat(paste0("\n   -> [1] อ่านไฟล์ได้: ", format(raw_row_count, big.mark=","), " แถว"))

  # Date Parsing
  date_formats <- c("ymd HMS", "dmy HMS", "d/m/Y H:M:S", "Y-m-d H:M:S", "d/m/Y H:M", "d-m-Y H:M:S", "dmY HM", "dbY HMS")

  if ("trxdatetime" %in% names(dt)) {
    dt[, t_ref := parse_date_time(trxdatetime, orders = date_formats, quiet=TRUE)]
  } else if ("entry_date" %in% names(dt)) {
    dt[, t_ref := parse_date_time(paste(entry_date, entry_time), orders = date_formats, quiet=TRUE)]
  } else if ("exit_time" %in% names(dt)) {
    dt[, t_ref := parse_date_time(exit_time, orders = date_formats, quiet=TRUE)]
  } else {
    dt[, t_ref := as.POSIXct(NA)]
  }

  dt_valid <- dt[!is.na(t_ref)]
  dropped_dates <- raw_row_count - nrow(dt_valid)
  if(dropped_dates > 0) cat(paste0("\n   ⚠️ หายไปตอนแปลงเวลา: ", format(dropped_dates, big.mark=","), " แถว"))

  dt <- dt_valid
  if(nrow(dt) == 0) { file.remove(temp_name); return(NULL) }

  # Cleansing Plate
  col_plate <- grep("plate|ทะเบียน", names(dt), value = TRUE, ignore.case = TRUE)[1]
  if (is.na(col_plate)) { file.remove(temp_name); return(NULL) }
  setnames(dt, col_plate, "plate_number")

  dt <- dt[plate_number != "" & !is.na(plate_number)]
  dt <- dt[!grepl("^UNKNOWN|^ไม่ทราบ|^ไม่มี|^TEST|^ADMIN|^VIP", plate_number, ignore.case = TRUE)]

  # Feature Engineering (รายเที่ยว)
  dt[, is_peak := fcase(hour(t_ref) %in% c(6, 7, 8, 16, 17, 18), 1, default = 0)]

  if("entry_time" %in% names(dt) & "exit_time" %in% names(dt)) {
     dt[, entry_t := parse_date_time(paste(entry_date, entry_time), orders = date_formats, quiet = TRUE)]
     dt[, exit_t := parse_date_time(exit_time, orders = date_formats, quiet = TRUE)]
     dt[, travel_duration := as.numeric(difftime(exit_t, entry_t, units = "mins"))]
  } else {
     dt[, travel_duration := NA_real_]
  }

  # Dynamic Column Detection (เพิ่มคีย์เวิร์ดการหาจังหวัด)
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
  dt[, actual_province := val_prov] # เก็บชื่อจังหวัดตัวเต็มไว้

  # ดึงเฉพาะคอลัมน์ที่จำเป็นออกไป (เพิ่ม actual_province)
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
      Freq_Per_Month = round(.N / 3, 2), # สมมติว่าเก็บข้อมูล 3 เดือน
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
    cat("🎉 อัปโหลดสำเร็จ! ไฟล์ FINAL_ALPR_Profiles_SPSS.csv พร้อมใช้งานใน Drive แล้วครับ\n")

} else {
    print("❌ ไม่พบข้อมูลที่ผ่านการกรอง")
}
