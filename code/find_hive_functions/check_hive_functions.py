import re

# Define a list of common Hive functions (as previously defined)
hive_functions = ['round', 'bround', 'floor', 'ceil', 'rand', 'exp', 'ln', 'log10', 'log2', 'log', 'pow', 'power', 'sqrt', 'bin', 'hex', 'unhex', 'conv', 'abs', 'pmod', 'sin', 'asin', 'cos', 'acos', 'tan', 'atan', 'degrees', 'radians', 'positive', 'negative', 'sign', 'e', 'pi', 'factorial', 'cbrt', 'shiftleft', 'shiftright', 'shiftrightunsigned', 'greatest', 'least', 'width_bucket', 'size', 'size', 'map_keys', 'map_values', 'array_contains', 'sort_array', 'binary', 'cast', 'from_unixtime', 'unix_timestamp', 'to_date', 'year', 'quarter', 'month', 'day', 'hour', 'minute', 'second', 'weekofyear', 'extract', 'datediff', 'date_add', 'date_sub', 'from_utc_timestamp', 'to_utc_timestamp', 'current_date', 'current_timestamp', 'add_months', 'last_day', 'next_day', 'trunc', 'months_between', 'date_format', 'if', 'isnull', 'isnotnull ', 'nvl', 'COALESCE', 'CASE', 'nullif', 'assert_true', 'ascii', 'base64', 'character_length', 'chr', 'concat', 'context_ngrams', 'concat_ws', 'decode', 'elt', 'encode', 'field', 'find_in_set', 'format_number', 'get_json_object', 'in_file', 'instr', 'length', 'locate', 'lower', 'lpad', 'ltrim', 'ngrams', 'octet_length', 'parse_url', 'printf', 'quote', 'regexp_extract', 'regexp_replace', 'repeat', 'replace', 'reverse', 'rpad', 'rtrim', 'sentences', 'space', 'split', 'str_to_map', 'substr', 'substr', 'substring_index', 'translate', 'trim', 'unbase64', 'upper', 'initcap', 'levenshtein', 'soundex', 'mask', 'mask_first_n', 'mask_last_n', 'mask_show_first_n', 'mask_show_last_n', 'mask_hash', 'java_method', 'reflect', 'hash', 'current_user', 'logged_in_user', 'current_database', 'md5', 'sha1', 'crc32', 'sha2', 'aes_encrypt', 'aes_decrypt', 'version', 'surrogate_key', 'count', 'sum', 'avg', 'min', 'max', 'variance', 'var_samp', 'stddev_pop', 'stddev_samp', 'covar_pop', 'covar_samp', 'corr', 'percentile', 'percentile_approx', 'regr_avgx', 'regr_avgy', 'regr_count', 'regr_intercept', 'regr_r2', 'regr_slope', 'regr_sxx', 'regr_sxy', 'regr_syy', 'histogram_numeric', 'collect_set', 'collect_list', 'ntile', 'explode', 'posexplode', 'inline', 'stack', 'json_tuple', 'parse_url_tuple', 'udf']

with open('input.txt', 'r') as file:
    input_text = file.read()

table_and_view_definitions = re.split(r'(CREATE TABLE|CREATE VIEW)', input_text, flags=re.IGNORECASE)
print("table_and_view_definitions")
print(table_and_view_definitions)
print("\n")

current_table_view_name = None

hive_function_pattern = r'\b(' + '|'.join(re.escape(func) for func in hive_functions) + r')\b'
print("Create hive_function_pattern")
print(hive_function_pattern + "\n")

for item in table_and_view_definitions:
    if item.strip().upper() == "CREATE TABLE" or item.strip().upper() == "CREATE VIEW":
        current_table_view_name = table_and_view_definitions[table_and_view_definitions.index(item) + 1].strip().split(' ')[0]
        print("current_table_view_name")
        print(current_table_view_name+ "\n")        
    else:
        matches = re.findall(hive_function_pattern, item, re.IGNORECASE)
        if matches:
            print("===============================")
            print(f"Table/View Name: {current_table_view_name}")
            print("Definition :"+ item.strip())
            print("\n")

