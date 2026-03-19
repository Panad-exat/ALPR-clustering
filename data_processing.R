# ==============================================================================
# BLOCK 1: SETUP & LIBRARIES (Optimized)
# ==============================================================================
print("⏳ Installing dependencies...")
system("apt-get update -qq")
system("apt-get install -y libcurl4-openssl-dev libssl-dev libxml2-dev libfontconfig1-dev")

pkgs <- c("googledrive", "data.table", "lubridate", "stringr")
new_pkgs <- pkgs[!(pkgs %in% installed.packages()[,"Package"])]
if(length(new_pkgs)) install.packages(new_pkgs)

library(googledrive)
library(data.table)
library(lubridate)
library(stringr)

# --- OPTIMIZATION SETTINGS ---
setDTthreads(0)                 # ใช้ CPU เต็มสูบ
options(googledrive_quiet = TRUE) # ✅ ปิด Warning: verbose แบบถูกวิธี

print("🔑 Authentication...")
drive_auth(use_oob = TRUE)

# ==============================================================================
# BLOCK 2: LOGIC FUNCTIONS
# ==============================================================================
tier3_keywords <- c("BENZ", "BMW", "VOLVO", "AUDI", "PORSCHE", "MINI", "LEXUS",
                    "TESLA", "LAND ROVER", "JAGUAR", "FERRARI", "LAMBORGHINI",
                    "MASERATI", "BENTLEY", "ROLLS", "ASTON", "MCLAREN", "LOTUS",
                    "ALFA ROMEO", "MAYBACH")

get_tier_score <- function(brand_vec, type_vec) {
  # ใช้ Vectorized Operation เพื่อความเร็วสูงสุด
  brand_clean <- str_to_upper(str_trim(brand_vec))
  type_clean <- str_to_upper(str_trim(type_vec))

  fcase(
    grepl("6|10|>10|รถบรรทุก|TRUCK|BUS|รถโดยสาร", type_clean), 2,
    grepl(paste(tier3_keywords, collapse = "|"), brand_clean), 3,
    default = 1
  )
}

# ==============================================================================
# BLOCK 3: FILE DISCOVERY
# ==============================================================================
target_folder <- "ALPR-Q3-Q4-Data"
folder_id <- drive_find(pattern = target_folder, type = "folder")

if(nrow(folder_id) == 0) stop("❌ Error: ไม่เจอ Folder!")

# ดึงรายชื่อไฟล์ 
files_list <- drive_ls(as_id(folder_id$id), pattern = "\\.csv$", n_max = 50000)
print(paste("✅ Ready to process:", nrow(files_list), "files"))

# ==============================================================================
# BLOCK 4 (Modified): EXTRACT MONTHLY LOGS
# ==============================================================================
print("--- 🚀 Starting Monthly Stats Extraction ---")

process_monthly_stats <- function(file_row) {

  temp_name <- paste0("temp_stats_", file_row$name)

  # 1. Download & Read
  tryCatch({ drive_download(as_id(file_row$id), path = temp_name, overwrite = TRUE) },
           error = function(e) return(NULL))

  if (!file.exists(temp_name)) return(NULL)

  # Fast Read (เลือกเฉพาะคอลัมน์ที่จำเป็นเพื่อประหยัด RAM)
  # เราต้องการแค่ ทะเบียน (เพื่อนับ User) และ เวลา (เพื่อระบุเดือน)
  dt <- tryCatch({ fread(temp_name, select = c("plate_number", "trxdatetime", "entry_date", "entry_time", "exit_time"), encoding = "UTF-8") },
                 error = function(e) { return(NULL) }) # ถ้าอ่านไม่ได้ข้ามเลย

  names(dt) <- tolower(names(dt))

  # 2. Date Parsing (Logic เดิม)
  date_formats <- c("ymd HMS", "dmy HMS", "d/m/Y H:M:S", "Y-m-d H:M:S")

  if ("trxdatetime" %in% names(dt)) {
    dt[, t_ref := parse_date_time(trxdatetime, orders = date_formats)]
  } else if ("entry_date" %in% names(dt)) {
    dt[, t_ref := parse_date_time(paste(entry_date, entry_time), orders = date_formats)]
  } else if ("exit_time" %in% names(dt)) {
    dt[, t_ref := parse_date_time(exit_time, orders = c(date_formats, "HM", "H:M:S"))]
  } else {
    dt[, t_ref := as.POSIXct(NA)]
  }

  # ตัดแถวที่ไม่มีเวลา (เพราะระบุเดือนไม่ได้)
  dt <- dt[!is.na(t_ref)]

  # สร้างคอลัมน์ "Month" (YYYY-MM)
  dt[, Month := format(t_ref, "%Y-%m")]

  # 3. แยกข้อมูลดิบ vs ข้อมูลกรอง (เพื่อทำสรุป)

  # --- ส่วนที่ A: นับ Transaction ดิบทั้งหมดในไฟล์นี้ (แยกตามเดือน) ---
  raw_counts <- dt[, .(Raw_Tx = .N), by = Month]

  # --- ส่วนที่ B: กรองข้อมูลเพื่อหา Valid Users ---
  # Filter 1: มีเลขทะเบียน
  dt_clean <- dt[plate_number != "" & !is.na(plate_number)]
  # Filter 2: ไม่ใช่ Unknown/Test (ตาม Code เดิมของคุณ)
  dt_clean <- dt_clean[!grepl("^UNKNOWN|^ไม่ทราบ|^ไม่มี|^TEST|^ADMIN|^VIP", plate_number, ignore.case = TRUE)]

  # Return แค่รายการทะเบียนที่ผ่านการกรอง พร้อมเดือน (ยังไม่นับ User ตรงนี้ เพราะรถ 1 คันอาจอยู่หลายไฟล์ในเดือนเดียวกัน)
  # เราจะส่งรายการ (Month, plate_number) ออกไปนับรวมข้างนอก
  output_list <- list(
    raw_stats = raw_counts,           # ยอดรวมดิบ
    clean_logs = dt_clean[, .(Month, plate_number)] # รายการรถที่ผ่านการกรอง
  )

  file.remove(temp_name)
  return(output_list)
}

# --- RUNC LOOP ---
stats_results <- lapply(1:nrow(files_list), function(i) {
  if (i %% 10 == 0) cat(paste0("\rCounting Stats: ", i, "/", nrow(files_list)))
  process_monthly_stats(files_list[i,])
})

# ลบรายการที่เป็น NULL
stats_results <- stats_results[!sapply(stats_results, is.null)]

# ==============================================================================
# BLOCK 5 (Modified): AGGREGATE & CALCULATE
# ==============================================================================
print("--- 📊 Calculating Final Monthly Statistics ---")

# 1. รวมยอด Raw Transaction (อันนี้บวกกันได้เลย)
all_raw_counts <- rbindlist(lapply(stats_results, `[[`, "raw_stats"))
final_raw_summary <- all_raw_counts[, .(Total_Raw_Transactions = sum(Raw_Tx)), by = Month]

# 2. รวมรายชื่อรถที่ผ่านการกรอง (เพื่อนำมานับ Unique User)
all_clean_logs <- rbindlist(lapply(stats_results, `[[`, "clean_logs"))

# นับจำนวนทะเบียนที่ไม่ซ้ำกัน ในแต่ละเดือน (uniqueN)
final_user_summary <- all_clean_logs[, .(Active_Unique_Users = uniqueN(plate_number)), by = Month]

# 3. รวมตารางเข้าด้วยกัน
final_monthly_stats <- merge(final_raw_summary, final_user_summary, by = "Month", all = TRUE)

# เรียงลำดับเดือน
setorder(final_monthly_stats, Month)

# แสดงผล
print(final_monthly_stats)

# Export
fwrite(final_monthly_stats, "Monthly_Traffic_Stats.csv")
print("✅ Stats Exported to Monthly_Traffic_Stats.csv")
