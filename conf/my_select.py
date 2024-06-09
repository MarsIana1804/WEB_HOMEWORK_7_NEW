from sqlalchemy import create_engine, select, func, desc, cast, Numeric
from sqlalchemy.orm import sessionmaker
from models import Student, Grade, Subject, Group, Teacher

# Connect to the database
engine = create_engine("postgresql+psycopg2://marina_admin:mar123@localhost:5433/homework_7")
Session = sessionmaker(bind=engine)
session = Session()

# Construct the query to find the top 5 students with the highest average grade
select_1 = (
    select(
        Student.name,
        cast(func.avg(Grade.grade), Numeric(10, 3)).label('average_grade')
    )
    .join(Grade, Student.id == Grade.student_id)
    .group_by(Student.id)
    .order_by(desc('average_grade'))
    .limit(5)
)

# Subquery to calculate the average grade for each student in each subject
subquery = (
    select(
        Grade.subject_id,
        Grade.student_id,
        cast(func.avg(Grade.grade), Numeric(10, 3)).label('average_grade')
    )
    .join(Student, Student.id == Grade.student_id)
    .join(Subject, Subject.id == Grade.subject_id)
    .group_by(Grade.subject_id, Grade.student_id)
).cte('avg_grades')

# CTE to rank students within each subject
ranked_subquery = (
    select(
        subquery.c.subject_id,
        subquery.c.student_id,
        subquery.c.average_grade,
        func.rank().over(
            partition_by=subquery.c.subject_id,
            order_by=desc(subquery.c.average_grade)
        ).label('rank')
    )
).cte('ranked_grades')

# Main query to select the student with the highest average grade for each subject
select_2 = (
    select(
        Subject.subject_name,
        Student.name.label('student_name'),
        ranked_subquery.c.average_grade
    )
    .join(Subject, Subject.id == ranked_subquery.c.subject_id)
    .join(Student, Student.id == ranked_subquery.c.student_id)
    .where(ranked_subquery.c.rank == 1)
)

select_3 = (
    select(
        Subject.subject_name,
        Group.group_name,
        cast(func.avg(Grade.grade), Numeric(10, 3)).label('average_grade')
    )
    .join(Subject, Subject.id == Grade.subject_id)
    .join(Student, Student.id == Grade.student_id)
    .join(Group, Group.id == Student.group_id)
    .group_by(Subject.subject_name, Group.group_name)
)

select_4 = select(cast(func.avg(Grade.grade), Numeric(10, 3)).label('average_grade'))

select_5 = (
    select(Subject.subject_name,
           Teacher.teacher_name)
    .join(Teacher,Teacher.id == Subject.teacher_id )   
)

select_6 = (
    select(Group.group_name, func.array_agg(Student.name))
    .join(Student, Group.id == Student.group_id)
    .group_by(Group.id)
)

select_7 = (
    select(
        Group.group_name,
        Subject.subject_name,
        Student.name.label('student_name'),
        Grade.grade
    )
    .join(Student, Group.id == Student.group_id)
    .join(Grade, Student.id == Grade.student_id)
    .join(Subject, Grade.subject_id == Subject.id)
    .order_by(Group.group_name, Subject.subject_name, Student.name)
)

select_8 = (
    select(
        Teacher.teacher_name,
        Subject.subject_name,
        cast(func.avg(Grade.grade), Numeric(10, 3)).label('average_grade')
    )
    .join(Subject, Teacher.id == Subject.teacher_id)
    .join(Grade, Subject.id == Grade.subject_id)
    .group_by(Teacher.teacher_name, Subject.subject_name)
    .order_by(Teacher.teacher_name, Subject.subject_name)
)

select_9 = (
    select(
        Student.name.label('student_name'),
        Subject.subject_name
    )
    .join(Grade, Student.id == Grade.student_id)
    .join(Subject, Grade.subject_id == Subject.id)
    .distinct()
    .order_by(Student.name, Subject.subject_name)
)

select_10 = (
    select(
        Teacher.teacher_name,
        Student.name.label('student_name'),
        Subject.subject_name,
        
    )
    .join(Grade, Student.id == Grade.student_id)
    .join(Subject, Grade.subject_id == Subject.id)
    .join(Teacher, Subject.teacher_id == Teacher.id)
    .distinct()
    .order_by(Teacher.teacher_name, Student.name, Subject.subject_name)
)

# Виконання запиту та отримання результатів


# Виведення результатів

result_1 = session.execute(select_1).fetchall()

result_2 = session.execute(select_2).fetchall()

result_3 = session.execute(select_3).fetchall()

result_4 = session.execute(select_4).scalar()

result_5 = session.execute(select_5).fetchall()

result_6 = session.execute(select_6).fetchall()

result_7 = session.execute(select_7).fetchall()

result_8 = session.execute(select_8).fetchall()

result_9 = session.execute(select_9).fetchall()

result_10 = session.execute(select_10).fetchall()


#1 Print the results
print("*********** SELECT_1 ***********")
for row in result_1:
    
    print(f"Student: {row.name}, Average Grade: {row.average_grade}")

#2 Print the results
print("*********** SELECT_2 ***********")
for row in result_2:
    
    print(f"Subject: {row.subject_name}, Student: {row.student_name}, Average Grade: {row.average_grade}")

#3 Print the results
print("*********** SELECT_3 ***********")
for row in result_3:
    
    print(f"Subject: {row.subject_name}, Group: {row.group_name}, Average Grade: {row.average_grade}")

#4 Print the results
print("*********** SELECT_4 ***********")
print(f"Average Grade Across All Grades: {result_4}")

#5 Print the subjects taught by the teacher
print("*********** SELECT_5 ***********")

for row in result_5:
    print(f"Teacher:{row.teacher_name}, subject: {row.subject_name}")

#6 Print the students in a group
print("*********** SELECT_6 ***********")
for row in result_6:
    print(f"Group: {row.group_name}, Students: {row.array_agg}")

#7 Print out the results
print("*********** SELECT_7 ***********")
for row in result_7:
    print(f"Group: {row.group_name}, Subject: {row.subject_name}, Student: {row.student_name}, Grade: {row.grade}")

#8 Print out the results
print("*********** SELECT_8 ***********")
for row in result_8:
    print(f"Teacher: {row.teacher_name}, Subject: {row.subject_name}, Average Grade: {row.average_grade}")


#9 Print out the results
print("*********** SELECT_9 ***********")
for row in result_9:
    print(f"Student: {row.student_name}, Subject: {row.subject_name}")

#10 Print out the results
print("*********** SELECT_10 ***********")
for row in result_10:
    print(f"Teacher: {row.teacher_name}, Subject: {row.subject_name}, Student: {row.student_name}")


# Close the session
session.close()
