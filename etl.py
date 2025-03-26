# etl.py
import pandas as pd
import pyodbc
from datetime import datetime, time

# Kết nối SQL Server
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=DESKTOP-QQJ6HV7\\SQLEXPRESS;'
    'DATABASE=ABCDatabase;'
    'Trusted_Connection=yes;'
    'TrustServerCertificate=yes;'
)

cursor = conn.cursor()

# Kiểm tra Employee_ID hợp lệ
def is_valid_employee(employee_id):
    employee_id = int(employee_id)
    cursor.execute("SELECT COUNT(*) FROM Employee WHERE Employee_ID = ?", employee_id)
    return cursor.fetchone()[0] > 0

# Hàm xác định trạng thái nghỉ phép dựa trên Check_In_Time và Check_Out_Time
def determine_leave_status(check_in, check_out):
    # Quy định công ty
    morning_threshold = time(10, 0)  # Đến sau 10h00 → Nghỉ buổi sáng
    afternoon_threshold = time(15, 0)  # Về trước 15h00 → Nghỉ buổi chiều

    # Nếu không có dữ liệu chấm công (check_in hoặc check_out là None)
    if check_in is None or check_out is None:
        return 'Full'

    # Chuyển check_in và check_out thành đối tượng time để so sánh
    check_in_time = check_in if isinstance(check_in, time) else check_in.time()
    check_out_time = check_out if isinstance(check_out, time) else check_out.time()

    # Xác định trạng thái nghỉ phép
    if check_in_time > morning_threshold:
        return 'Morning'
    elif check_out_time < afternoon_threshold:
        return 'Afternoon'
    else:
        return 'None'

# 1. Đọc dữ liệu thô từ máy quét vân tay
data = {
    'Person ID': [1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3],
    'Name': ['Nam', 'Nam', 'Nam', 'Nam', 'Nam', 'Lan', 'Lan', 'Lan', 'Lan', 'Hao', 'Hao'],
    'Department': ['QA', 'QA', 'QA', 'QA', 'QA', 'Dev', 'Dev', 'Dev', 'Dev', 'Dev', 'Dev'],
    'Time': ['09/04/2024 08:00', '09/04/2024 12:00', '09/04/2024 17:00', 
             '10/04/2024 13:30', '10/04/2024 17:30',
             '09/04/2024 08:30', '09/04/2024 17:30', 
             '10/04/2024 08:00', '10/04/2024 12:00', 
             '10/04/2024 08:00', '10/04/2024 12:00'],
    'Attendance Status': ['None', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None', 'None'],
    'Attendance Check Point': ['Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1', 'Door1'],
    'Custom Name': ['', '', '', '', '', '', '', '', '', '', ''],
    'Data Source': ['Original Records', 'Original Records', 'Original Records', 
                    'Original Records', 'Original Records', 'Original Records', 
                    'Original Records', 'Original Records', 'Original Records', 'Original Records', 'Original Records'],
    'Handling Type': ['', '', '', '', '', '', '', '', '', '', ''],
    'Temperature': ['', '', '', '', '', '', '', '', '', '', ''],
    'Abnormal': ['', '', '', '', '', '', '', '', '', '', '']
}

df = pd.DataFrame(data)

# 2. Xử lý dữ liệu (ETL)
df['Time'] = pd.to_datetime(df['Time'], format='%d/%m/%Y %H:%M')

df['Date'] = df['Time'].dt.date
grouped = df.groupby(['Person ID', 'Date'])

attendance_records = []
for (person_id, date), group in grouped:
    if not is_valid_employee(person_id):
        print(f"Nhân viên {person_id} không tồn tại trong bảng Employee, bỏ qua.")
        continue
    
    check_in = group['Time'].min()
    check_out = group['Time'].max()
    
    # Tự động xác định trạng thái nghỉ phép
    leave = determine_leave_status(check_in, check_out)
    
    # Điều chỉnh Check_In_Time và Check_Out_Time dựa trên trạng thái nghỉ phép
    if leave == 'Morning':
        check_in = max(check_in, check_in.replace(hour=13, minute=30))
        check_out = max(check_out, check_in)
    elif leave == 'Afternoon':
        check_out = min(check_out, check_out.replace(hour=12, minute=0))
        check_in = min(check_in, check_out)
    elif leave == 'Full':
        work_hours = 0
        attendance_records.append({
            'Employee_ID': person_id,
            'Date': date,
            'Check_In_Time': None,
            'Check_Out_Time': None,
            'Work_Hours': work_hours,
            'Leave_Status': leave
        })
        continue
    
    work_hours = (check_out - check_in).total_seconds() / 3600
    lunch_start = check_in.replace(hour=12, minute=0)
    lunch_end = check_in.replace(hour=13, minute=30)
    if check_in < lunch_start and check_out > lunch_end:
        work_hours -= 1.5
    if leave in ['Morning', 'Afternoon']:
        work_hours = min(work_hours, 4)
    
    attendance_records.append({
        'Employee_ID': person_id,
        'Date': date,
        'Check_In_Time': check_in.time() if check_in else None,
        'Check_Out_Time': check_out.time() if check_out else None,
        'Work_Hours': round(work_hours, 2),
        'Leave_Status': leave
    })

attendance_df = pd.DataFrame(attendance_records)
print("Dữ liệu sau khi xử lý ETL:")
print(attendance_df)

# 3. Lưu dữ liệu vào SQL Server
cursor.execute("TRUNCATE TABLE Attendance")
for index, row in attendance_df.iterrows():
    cursor.execute("""
        INSERT INTO Attendance (Employee_ID, Date, Check_In_Time, Check_Out_Time, Work_Hours, Leave_Status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, row['Employee_ID'], row['Date'], row['Check_In_Time'], row['Check_Out_Time'], 
           row['Work_Hours'], row['Leave_Status'])

conn.commit()
print("Dữ liệu đã được lưu vào SQL Server.")

cursor.close()
conn.close()