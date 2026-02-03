# standardize columns

def standardize_columns(input_data):
    # Convert all column names to lowercase
    input_data.columns = input_data.columns.str.lower()
    
    # Replace white spaces with underscores in column names
    input_data.columns = input_data.columns.str.replace(' ', '_')
    
    return input_data

EXCHANGE_RATES = {
    "EUR": 1.0,
    "SEK": 1 / 10.53,  # ≈ 0.09496
    "NOK": 1 / 11.38   # ≈ 0.08787
}
