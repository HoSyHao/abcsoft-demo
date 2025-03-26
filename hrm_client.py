# hrm_client.py
import requests

BASE_URL = "http://localhost:5000/api"

def get_employee(employee_id):
    response = requests.get(f"{BASE_URL}/employee/{employee_id}")
    return response.json()

def get_total_hours(employee_id, month):
    response = requests.get(f"{BASE_URL}/attendance/total_hours/{employee_id}/{month}")
    return response.json()

def get_payroll(employee_id, month):
    response = requests.get(f"{BASE_URL}/payroll/{employee_id}/{month}")
    return response.json()

if __name__ == "__main__":
    print("Báo cáo chấm công và lương tháng 4:")
    for emp_id in [1, 2, 3]:
        emp_info = get_employee(emp_id)
        if 'error' in emp_info:
            print(f"Nhân viên {emp_id}: {emp_info['error']}")
            continue
        
        hours = get_total_hours(emp_id, "4")
        if 'error' in hours:
            print(f"Nhân viên {emp_id}: {hours['error']}")
            continue
        
        payroll = get_payroll(emp_id, "4")
        if 'error' in payroll:
            print(f"Nhân viên {emp_id}: {payroll['error']}")
            continue
        
        print(f"\nNhân viên: {emp_info['first_name']} {emp_info['last_name']}")
        print(f"Phòng ban: {emp_info['department']}")
        print(f"Công việc: {emp_info['job_title']}")
        print(f"Tổng giờ làm việc: {hours['total_hours']} giờ")
        print(f"Lương: {payroll['total_pay']} USD (Mức lương: {emp_info['hourly_rate']} USD/giờ)")