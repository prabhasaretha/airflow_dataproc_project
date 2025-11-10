from pyspark.sql import SparkSession

def sparkfunc():
    # Create SparkSession properly
    spark = SparkSession.builder.appName('First Airflow Project').getOrCreate()

    bucket = 'airflow_demo_files'
    input_path = f'gs://{bucket}/input_data/employee.csv'
    output_path = f'gs://{bucket}/output_data/'

    # Read CSV
    employee_data = spark.read.csv(input_path, header=True, inferSchema=True)

    # Filter data
    filtered_employee = employee_data.filter(employee_data.salary >= 60000)

    # Write output
    filtered_employee.write.mode('overwrite').csv(output_path, header=True)

    print(f'Filtered data written to {output_path}')

if __name__ == "__main__":
    sparkfunc()
