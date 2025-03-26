# api_gateway.py
from flask import Flask, jsonify
import pyodbc
import redis
import json

app = Flask(__name__)

cache = redis.Redis(host='localhost', port=6379, db=0)

def get_db_connection():
    return pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=DESKTOP-QQJ6HV7\\SQLEXPRESS;'
        'DATABASE=ABCDatabase;'
        'Trusted_Connection=yes;'
        'TrustServerCertificate=yes;'
    )

# API lấy thông tin nhân viên
@app.route('/api/employee/<int:employee_id>', methods=['GET'])
def get_employee(employee_id):
    cache_key = f"employee_{employee_id}"
    cached_data = cache.get(cache_key)
    if cached_data:
        print(f"Truy xuất từ cache: {cache_key}")
        return jsonify(json.loads(cached_data))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT e.First_Name, e.Last_Name, e.Email, d.Department_Name, j.Job_Title, e.Hourly_Rate, e.Status
        FROM Employee e
        LEFT JOIN Departments d ON e.Department_ID = d.Department_ID
        LEFT JOIN Jobs j ON e.Job_ID = j.Job_ID
        WHERE e.Employee_ID = ?
    """, employee_id)
    result = cursor.fetchone()
    
    if not result:
        return jsonify({'error': 'Nhân viên không tồn tại'}), 404
    
    response = {
        'employee_id': employee_id,
        'first_name': result[0],
        'last_name': result[1],
        'email': result[2],
        'department': result[3],
        'job_title': result[4],
        'hourly_rate': float(result[5]),
        'status': result[6]
    }
    
    cache.setex(cache_key, 60, json.dumps(response))
    
    cursor.close()
    conn.close()
    
    return jsonify(response)

# API lấy tổng giờ làm việc
@app.route('/api/attendance/total_hours/<int:employee_id>/<string:month>', methods=['GET'])
def get_total_hours(employee_id, month):
    cache_key = f"total_hours_{employee_id}_{month}"
    cached_data = cache.get(cache_key)
    if cached_data:
        print(f"Truy xuất từ cache: {cache_key}")
        return jsonify(json.loads(cached_data))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT Status FROM Employee WHERE Employee_ID = ?", employee_id)
    result = cursor.fetchone()
    if not result or result[0] != 'Active':
        return jsonify({'error': 'Nhân viên không tồn tại hoặc không hoạt động'}), 404
    
    cursor.execute("""
        SELECT SUM(Work_Hours) AS Total_Hours
        FROM Attendance
        WHERE Employee_ID = ? AND MONTH(Date) = ? AND YEAR(Date) = 2024
    """, (employee_id, int(month)))
    result = cursor.fetchone()
    
    total_hours = result[0] if result[0] is not None else 0
    response = {'employee_id': employee_id, 'month': month, 'total_hours': float(total_hours)}
    cache.setex(cache_key, 60, json.dumps(response))
    
    cursor.close()
    conn.close()
    
    return jsonify(response)

# API lấy bảng lương
@app.route('/api/payroll/<int:employee_id>/<string:month>', methods=['GET'])
def get_payroll(employee_id, month):
    cache_key = f"payroll_{employee_id}_{month}"
    cached_data = cache.get(cache_key)
    if cached_data:
        print(f"Truy xuất từ cache: {cache_key}")
        return jsonify(json.loads(cached_data))
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT Status, Hourly_Rate FROM Employee WHERE Employee_ID = ?", employee_id)
    result = cursor.fetchone()
    if not result or result[0] != 'Active':
        return jsonify({'error': 'Nhân viên không tồn tại hoặc không hoạt động'}), 404
    
    hourly_rate = result[1]
    
    cursor.execute("""
        SELECT SUM(Work_Hours) AS Total_Hours
        FROM Attendance
        WHERE Employee_ID = ? AND MONTH(Date) = ? AND YEAR(Date) = 2024
    """, (employee_id, int(month)))
    result = cursor.fetchone()
    
    total_hours = result[0] if result[0] is not None else 0
    total_pay = total_hours * hourly_rate
    
    response = {
        'employee_id': employee_id,
        'month': month,
        'total_hours': float(total_hours),
        'total_pay': float(total_pay)
    }
    
    cache.setex(cache_key, 60, json.dumps(response))
    
    cursor.close()
    conn.close()
    
    return jsonify(response)

if __name__ == '__main__':
    app.run(debug=True, port=5000)