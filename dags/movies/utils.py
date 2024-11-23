import csv

def count_lines(filename, delimiter=","):
    with open(filename, "r", encoding='utf-8') as file:
        return sum(
            1 for line in file 
            if any(field.strip() for field in line.split(delimiter))  # Only count lines with non-empty fields
        )
        
        

