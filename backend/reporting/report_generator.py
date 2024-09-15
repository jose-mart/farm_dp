from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, sum as spark_sum, col
import os

def generate_report(spark:SparkSession, report_date: str, milk_production_df: DataFrame, cows_weight_df: DataFrame, cows_health_status_df: DataFrame):
    # Filter data for the given date
    milk_production_filtered = milk_production_df.filter(col("date") == report_date)
    cows_weight_filtered = cows_weight_df.filter(col("date") == report_date)
    
    # Summary for Milk Production
    milk_production_summary = milk_production_filtered.groupBy("cow_id").agg(
        avg("total_milk_production").alias("avg_daily_milk_production"),
        spark_sum("total_milk_production").alias("total_milk_production")
    ).collect()

    # Total milk production for the day
    total_milk_production_day = milk_production_filtered.agg(spark_sum("total_milk_production").alias("total_milk_production")).collect()[0]["total_milk_production"]
    
    # Summary for Cow Weights
    cows_weight_summary = cows_weight_filtered.groupBy("cow_id").agg(
        avg("current_weight").alias("current_weight"),
        avg("average_weight_last_30_days").alias("avg_weight_last_30_days")
    ).collect()

    # Average weight of all cows
    if cows_weight_filtered.count() == 0:
        average_weight_all_cows = -1
    else:
        average_weight_all_cows = cows_weight_filtered.agg(avg("current_weight").alias("avg_weight")).collect()[0]["avg_weight"]
    
    # Cow Health Status
    health_status_summary = cows_health_status_df.collect()
    
    # Count cows with good health and anomalies
    good_health_cows = [row['cow_id'] for row in health_status_summary if row['milk_anomaly'] == 0 and row['weight_anomaly'] == 0]
    milk_anomaly_count = sum([row['milk_anomaly'] for row in health_status_summary])
    weight_anomaly_count = sum([row['weight_anomaly'] for row in health_status_summary if isinstance(row["weight_anomaly"], int)])
    
    # Generate Text Report
    report = []
    report.append(f"Report Title: Daily Cow Health and Production Report")
    report.append(f"Date: {report_date}\n")
    
    report.append("Milk Production Summary:")
    report.append("-------------------------")
    report.append(f"- Total Milk Production for {report_date}: {total_milk_production_day:.2f} liters\n")
    for row in milk_production_summary:
        report.append(f"Cow ID: {row['cow_id']}")
        report.append(f"- Average Daily Milk Production: {row['avg_daily_milk_production']:.2f} liters")
        report.append(f"- Total Milk Production: {row['total_milk_production']:.2f} liters")
        report.append("")
    
    report.append("Cow Weight Summary:")
    report.append("-------------------")
    report.append(f"- Average Weight of All Cows: {average_weight_all_cows:.2f} kg")
    for row in cows_weight_summary:
        report.append(f"Cow ID: {row['cow_id']}")
        report.append(f"- Current Weight: {row['current_weight']:.2f} kg")
        report.append(f"- Average Weight Last 30 Days: {row['avg_weight_last_30_days']:.2f} kg")
        report.append("")
    
    report.append("Cow Health Status:")
    report.append("------------------")
    report.append(f"- Number of Cows with Good Health: {len(good_health_cows)}")
    report.append(f"- Number of Cows with Milk Anomalies: {milk_anomaly_count}")
    report.append(f"- Number of Cows with Weight Anomalies: {weight_anomaly_count}")
    
    return "\n".join(report)


def save_report(report_date: str, report_content: str):
    # Define the report folder
    report_folder = 'reports'
    
    try:
        # Create the folder if it doesn't exist
        if not os.path.exists(report_folder):
            os.makedirs(report_folder)
        
        # Create a filename based on the date
        filename = os.path.join(report_folder, f"report_{report_date}.txt")
        
        # Write the report content to the file (overwrite if exists)
        with open(filename, 'w') as file:
            file.write(report_content)
        
        print(f"Report successfully saved to {filename}")
    
    except Exception as e:
        print(f"Error saving report: {str(e)}")