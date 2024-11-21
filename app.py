import sqlite3
import json
import pandas as pd 

def flatten_dataframe(df):
    """
    Flattens the nested JSON in PromotionDiscount column and performs data type conversions
    
    Args:
        df (pandas.DataFrame): Input DataFrame containing PromotionDiscount column
    
    Returns:
        pandas.DataFrame: Flattened DataFrame with JSON data normalized
    """
    # Convert JSON strings in PromotionDiscount to Python dictionaries
    df['PromotionDiscount'] = df['PromotionDiscount'].apply(json.loads)
    
    # Normalize the JSON column and concatenate with original dataframe
    df_expanded = pd.concat([df, pd.json_normalize(df['PromotionDiscount'])], axis=1)

    # Convert Amount column to float type for calculations
    df_expanded['Amount'] = df_expanded['Amount'].astype('float')
    
    # Remove the original PromotionDiscount column
    return df_expanded.drop('PromotionDiscount', axis=1)

def read_csv_files(region_a_path, region_b_path):
    """
    Reads and processes Excel files from two regions
    
    Args:
        region_a_path (str): File path for Region A Excel file
        region_b_path (str): File path for Region B Excel file
    
    Returns:
        tuple: Two DataFrames containing processed data for each region
    """
    # Read Excel files into DataFrames
    region_a_df = pd.read_excel(region_a_path)
    region_b_df = pd.read_excel(region_b_path)

    # Process Region A data
    flatten_region_a_df = flatten_dataframe(region_a_df)
    flatten_region_a_df['region'] = 'A'  # Add region identifier
    
    # Process Region B data
    flatten_region_b_df = flatten_dataframe(region_b_df)
    flatten_region_b_df['region'] = 'B'  # Add region identifier

    return flatten_region_a_df, flatten_region_b_df

def transform_data(df_region_a, df_region_b):
    """
    Combines and transforms data from both regions
    
    Args:
        df_region_a (pandas.DataFrame): Processed data from Region A
        df_region_b (pandas.DataFrame): Processed data from Region B
    
    Returns:
        pandas.DataFrame: Combined and transformed DataFrame
    """
    # Combine datasets from both regions
    combined_df = pd.concat([df_region_a, df_region_b])
    
    # Calculate total sales (Quantity * Price)
    combined_df["total_sales"] = combined_df["QuantityOrdered"] * combined_df["ItemPrice"] 
    
    # Calculate net sales (total sales - discount amount)
    combined_df["net_sale"] = combined_df["total_sales"] - combined_df["Amount"] 
    
    # Remove duplicate orders
    transform_df = combined_df.drop_duplicates(["OrderId"])
    
    # Filter out orders with negative or zero net sales
    transform_df = transform_df[transform_df["net_sale"] > 0]
        
    return transform_df

def load_to_sqlite(df, db_path="sales_data.db"):
    """
    Loads the transformed data into SQLite database
    
    Args:
        df (pandas.DataFrame): Transformed DataFrame to be loaded
        db_path (str): Path to SQLite database file
    """
    # Create connection to SQLite database
    conn = sqlite3.connect(db_path)
    
    # Write DataFrame to SQL table, replace if exists
    df.to_sql("sales_data", conn, if_exists="replace", index=False)
    
    # Close database connection
    conn.close()

def main():
    """
    Main function to execute the ETL pipeline
    """
    # Define input file paths
    region_a_path = '/Users/vedprakash/Desktop/sales_analysis/input/order_region_a.xlsx'
    region_b_path = '/Users/vedprakash/Desktop/sales_analysis/input/order_region_b.xlsx'

    # Extract: Read and process input files
    df_region_a, df_region_b = read_csv_files(region_a_path, region_b_path)
    
    # Transform: Combine and transform the data
    transformed_df = transform_data(df_region_a, df_region_b)
    
    # Load: Save processed data to SQLite database
    load_to_sqlite(transformed_df)

# Execute main function if script is run directly
if __name__ == '__main__':
    main()