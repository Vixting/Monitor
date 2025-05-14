import re

def extract_wave_number(wave_text):
    if not wave_text:
        return None
   
    match = re.search(r'Wave\s*(\d+)', wave_text, re.IGNORECASE)
    if match:
        return int(match.group(1))
   
    numbers = re.findall(r'\d+', wave_text)
    if numbers:
        return int(numbers[0])
   
    return None