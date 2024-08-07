from pipeline.main import clean_data_stats

def test_clean_data_stats():
    test_input = {'Duis magna est inc': 2,
                  'sold': 9,
                  'pending': 13, 
                  'available': 243, 
                  'invalid': 7, 
                  'Not Available': 1, 
                  'Available': 1,
                  'adopted': 1, 'Unavailable': 1}
    
    result_output = clean_data_stats.fn(test_input)
    expected_output = {
        "sold": 9,
        "pending": 13,
        "available": 244,  
        "not available": 1  
    }
    
    print("Result Output:", result_output)
    print("Expected Output:", expected_output)
    
    assert result_output == expected_output
