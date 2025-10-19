import datetime
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MSSQLConnection:
    def __init__(self, server, database, username, password):
        self.connection_string = (
            f"DRIVER={{ODBC Driver 17 for SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=no;"
        )
        self.connection = None
    
    def connect(self):
        try:
            self.connection = pyodbc.connect(self.connection_string)
            logger.info("Connected")
            return True
        except pyodbc.Error as e:
            logger.error(f"Connection errpr: {e}")
            return False
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
            logger.info("Disconnected")
    
    
    def get_students(self, limit=10):
        try:
            cursor = self.connection.cursor()
            cursor.execute(f"SELECT TOP ({limit}) * FROM [Student].[dbo].[StudentTest]")
            students = cursor.fetchall()
            
            result = []
            for student in students:
                result.append({
                    'St_ID': student[0],
                    'Recordbook': student[1],
                    'First_Name': student[2],
                    'Last_Name': student[3],
                    'Date_of_Birth': student[4],
                    'Enter_Date': student[5]
                })
            
            return result
        except pyodbc.Error as e:
            logger.error(f"Error during retrieving data: {e}")
            return []
    
    def get_student_by_id(self, st_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("SELECT * FROM [Student].[dbo].[StudentTest] WHERE St_ID = ?", st_id)
            student = cursor.fetchone()
            
            if student:
                return {
                    'St_ID': student[0],
                    'Recordbook': student[1],
                    'First_Name': student[2],
                    'Last_Name': student[3],
                    'Date_of_Birth': student[4],
                    'Enter_Date': student[5]
                }
            return None
        except pyodbc.Error as e:
            logger.error(f"Error: {e}")
            return None
    
  
    def insert_student(self, recordbook, first_name, last_name, date_of_birth, enter_date=None):
        try:
            cursor = self.connection.cursor()
            
            if enter_date is None:
                enter_date = datetime.now()
            
            cursor.execute(
                """INSERT INTO [Student].[dbo].[StudentTest] 
                (Recordbook, First_Name, Last_Name, Date_of_Birth, Enter_Date) 
                VALUES (?, ?, ?, ?, ?)""",
                recordbook, first_name, last_name, date_of_birth, enter_date
            )
            
            cursor.execute("SELECT @@IDENTITY")
            identity_id = cursor.fetchone()[0]
            return identity_id
                    
        except pyodbc.Error as e:
            logger.error(f"Error during insertion: {e}")
            if self.connection:
                self.connection.rollback()
            return None
    
    def update_student(self, st_id, recordbook=None, first_name=None, 
                      last_name=None, date_of_birth=None, enter_date=None):
        try:
            cursor = self.connection.cursor()
            update_fields = []
            params = []
            if recordbook is not None:
                update_fields.append("Recordbook = ?")
                params.append(recordbook)
            if first_name:
                update_fields.append("First_Name = ?")
                params.append(first_name)
            if last_name:
                update_fields.append("Last_Name = ?")
                params.append(last_name)
            if date_of_birth:
                update_fields.append("Date_of_Birth = ?")
                params.append(date_of_birth)
            if enter_date:
                update_fields.append("Enter_Date = ?")
                params.append(enter_date)
            
            if not update_fields:
                return 0
            
            params.append(st_id)
            query = f"UPDATE [Student].[dbo].[StudentTest] SET {', '.join(update_fields)} WHERE St_ID = ?"
            
            cursor.execute(query, params)
            self.connection.commit()
            logger.info(f"Student {st_id} was updated")
            return cursor.rowcount
        except pyodbc.Error as e:
            logger.error(f"Error during updating: {e}")
            return 0
    
    def delete_student(self, st_id):
        try:
            cursor = self.connection.cursor()
            cursor.execute("DELETE FROM [Student].[dbo].[StudentTest] WHERE St_ID = ?", st_id)
            self.connection.commit()
            logger.info(f"Student {st_id} was deleted")
            return cursor.rowcount
        except pyodbc.Error as e:
            logger.error(f"Error: {e}")
            return 0

def main():
    config = {
        'server': 'localhost\\NEWMSSQLSERVER',  
        'database': 'Student',
        'username': 'SA',
        'password': 'mssqlserver'
    }
    
    db = MSSQLConnection(**config)
    
    if db.connect():
        students = db.get_students()
        print("All students before insert:")
        for student in students:
            print(student)

        new_student_id = db.insert_student(
            recordbook="1004",
            first_name="Olha",
            last_name="Shevchenko",
            date_of_birth=datetime.date(2000, 5, 14),
            enter_date=datetime.date(2018, 9, 1)
        )
        print(f"\nInserted new student with ID {new_student_id}")

        new_student = db.get_student_by_id(new_student_id)
        print("\nNewly inserted student:")
        print(new_student)

        for student in students:
            print(student)

        if students:
            db.update_student(
                st_id=new_student_id,
                recordbook = "1010"
            )
            updated_student = db.get_student_by_id(new_student_id)
            print(f"\nUpdated student with ID {new_student_id}:")
            print(updated_student)

        if students:
            try:
                db.delete_student(new_student_id)
                print(f"\nDeleted student with ID {new_student_id}")
            except Exception as e:
                print(f"\nCannot delete student with ID {new_student_id}: {e}")

        students_after = db.get_students()
        print("\nStudents after operations:")
        for student in students_after:
            print(student)
        

        db.disconnect()

if __name__ == "__main__":
    main()