# standard lib
import logging
import xlsxwriter
from lxml import etree
import datetime
import os
import operator

# common lib
from common import log_setup
from common import log_session_info
from common import is_file
from common import compress_whitespace
from common import make_name


class ProfileRow:

	def __init__(self, schema_name, table_name, column_name):
		# schema.table.column
		self.schema_name = schema_name
		self.table_name = table_name
		self.column_name = column_name

		# profile stats
		self.null_count = ''
		self.min_value = ''
		self.max_value = ''
		self.mean_value = ''
		self.std_dev = ''
		self.min_len = ''
		self.max_len = ''
		self.len_1 = ''
		self.len_count_1 = ''
		self.len_2 = ''
		self.len_count_2 = ''
		self.len_2 = ''
		self.len_count_2 = ''
		self.len_3 = ''
		self.len_count_3 = ''
		self.len_4 = ''
		self.len_count_4 = ''
		self.len_5 = ''
		self.len_count_5 = ''
		self.len_6 = ''
		self.len_count_6 = ''
		self.pattern_1 = ''
		self.pattern_1_count = ''
		self.pattern_2 = ''
		self.pattern_2_count = ''
		self.pattern_3 = ''
		self.pattern_3_count = ''
		self.pattern_4 = ''
		self.pattern_4_count = ''
		self.pattern_5 = ''
		self.pattern_5_count = ''
		#self.pattern_1_percent = ''
		self.distinct_values = ''
		self.value_dist_1 = ''
		self.value_dist_count_1 = ''
		self.value_dist_2 = ''
		self.value_dist_count_2 = ''
		self.value_dist_3 = ''
		self.value_dist_count_3 = ''
		self.value_dist_4 = ''
		self.value_dist_count_4 = ''
		self.value_dist_5 = ''
		self.value_dist_count_5 = ''
		self.is_exact_key = ''
		self.key_strength = ''
		self.key_value_1 = ''
		self.key_value_count_1 = ''
		self.key_value_2 = ''
		self.key_value_count_2 = ''
		self.key_value_3 = ''
		self.key_value_count_3 = ''
		self.key_value_4 = ''
		self.key_value_count_4 = ''
		self.key_value_5 = ''
		self.key_value_count_5 = ''


if __name__ == '__main__':
	log_setup(log_level=logging.INFO)
	log_session_info()
	rows = dict()

	tree = etree.parse('C:/output/all_output.xml')
	# tree = etree.parse('C:/output/access_output.xml')
	root = tree.getroot()
	data_profile_elem = root.find('DataProfileOutput')
	profile_elem = data_profile_elem.find('Profiles')

	# Iterate through all child of the <Profiles> node
	for output_elem in profile_elem.iterchildren():
		if output_elem.tag == 'ColumnNullRatioProfile':
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('Column')
			null_count = output_elem.find('NullCount').text

			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			key = (schema_name, table_name, column_name)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			# now fill-in row attributes
			row.null_count = null_count

		elif output_elem.tag == 'ColumnStatisticsProfile':
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('Column')
			min_value = output_elem.find('MinValue').text
			max_value = output_elem.find('MaxValue').text

			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			mean_value = ''
			std_dev = ''

			if output_elem.find('Mean') is not None:
				mean_value = output_elem.find('Mean').text

			if output_elem.find('StdDev') is not None:
				std_dev = output_elem.find('StdDev').text

			key = (schema_name, table_name, column_name)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			row.mean_value = mean_value
			row.min_value = min_value
			row.max_value = max_value
			row.std_dev = std_dev

		elif output_elem.tag == 'ColumnLengthDistributionProfile':

			# Retrieve Table and Column elements
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('Column')

			# Parse out schema_name, table_name and column_name
			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			# Parse out values
			min_len = output_elem.find('MinLength').text
			max_len = output_elem.find('MaxLength').text

			# Create key tuple
			key = (schema_name, table_name, column_name)

			# Add new row to rows else update existing row (based on key)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			len_dist_parent_elem = output_elem.find('LengthDistribution')

			# Find count/length distributions
			len_count_list = []

			for len_dist_child_elem in len_dist_parent_elem.iterchildren():
				length = len_dist_child_elem.find('Length').text
				count = len_dist_child_elem.find('Count').text
				len_count = (int(length), int(count))
				len_count_list.append(len_count)

			# Sort the len_count_list by the length distribution count value (highest to lowest)
			len_count_list.sort(key=operator.itemgetter(1), reverse=True)

			if len(len_count_list) <= 6:
				# set row len and len_count properties for all values in len_count_list
				for len_count_index, len_count_value in enumerate(len_count_list, 1):
					setattr(row, f'len_{len_count_index}', len_count_value[0])
					setattr(row, f'len_count_{len_count_index}', len_count_value[1])
			else:
				# set row len and len_count properties for first 3 and last 3 indexes in len_count_list
				for len_count_index2, len_count_value in enumerate(len_count_list[:3], 1):
					setattr(row, f'len_{len_count_index2}', len_count_value[0])
					setattr(row, f'len_count_{len_count_index2}', len_count_value[1])

				for len_count_index3, len_count_value in enumerate(len_count_list[len(len_count_list)-3:], 4):
					setattr(row, f'len_{len_count_index3}', len_count_value[0])
					setattr(row, f'len_count_{len_count_index3}', len_count_value[1])

			row.min_len = min_len
			row.max_len = max_len

		elif output_elem.tag == 'ColumnPatternProfile':
			# Retrieve Table and Column elements
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('Column')

			# Parse out schema_name, table_name and column_name
			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			# Create key tuple
			key = (schema_name, table_name, column_name)

			# Add new row to rows else update existing row (based on key)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			pattern_parent_elem = output_elem.find('TopRegexPatterns')

			# Find count/length distributions
			pattern_freq_list = []

			for pattern_freq_child_elem in pattern_parent_elem.iterchildren():
				pattern = pattern_freq_child_elem.find('RegexText').text
				frequency = pattern_freq_child_elem.find('Frequency').text
				pattern_freq_tuple = (pattern, f'{frequency}%')
				pattern_freq_list.append(pattern_freq_tuple)

			# Sort the len_count_list by the length distribution count value (highest to lowest)
			pattern_freq_list.sort(key=operator.itemgetter(1), reverse=True)

			for pattern_index, pattern_freq_value in enumerate(pattern_freq_list[0:4], 1):
				setattr(row, f'pattern_{pattern_index}', pattern_freq_value[0])
				setattr(row, f'pattern_{pattern_index}_count', pattern_freq_value[1])

		elif output_elem.tag == 'ColumnValueDistributionProfile':
			# Retrieve Table and Column elements
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('Column')

			# Parse out schema_name, table_name and column_name
			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			# Create key tuple
			key = (schema_name, table_name, column_name)

			# Add new row to rows else update existing row (based on key)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			# Apply distinct_value to Row
			distinct_value = int(output_elem.find('NumberOfDistinctValues').text)
			row.distinct_value = distinct_value

			# Find parent element
			value_dist_parent_elem = output_elem.find('ValueDistribution')

			# Set up list to hold values return from child elements
			value_dist_list = []
			if value_dist_parent_elem is not None:
				for value_dist_child_elem in value_dist_parent_elem.iterchildren():
					value = value_dist_child_elem.find('Value').text
					count = value_dist_child_elem.find('Count').text
					value_dist_tuple = (value, int(count))
					value_dist_list.append(value_dist_tuple)

			# Sort the len_count_list by the length distribution count value (highest to lowest)
			value_dist_list.sort(key=operator.itemgetter(1), reverse=True)

			for value_dist_index, value_dist in enumerate(value_dist_list[0:4], 1):
				setattr(row, f'value_dist_{value_dist_index}', value_dist[0])
				setattr(row, f'value_dist_count_{value_dist_index}', value_dist[1])

		elif output_elem.tag == 'CandidateKeyProfile':
			# Retrieve Table and Column elements
			table_elem = output_elem.find('Table')
			column_elem = output_elem.find('KeyColumns').find('Column')

			# Parse out schema_name, table_name and column_name
			schema_name = table_elem.attrib['Schema']
			table_name = table_elem.attrib['Table']
			column_name = column_elem.attrib['Name']

			# Create key tuple
			key = (schema_name, table_name, column_name)

			# Add new row to rows else update existing row (based on key)
			if key in rows:
				row = rows[key]
			else:
				row = ProfileRow(schema_name, table_name, column_name)
				rows[key] = row

			# Apply distinct_value to Row
			is_exact_key = output_elem.find('IsExactKey').text
			row.is_exact_key = is_exact_key

			if is_exact_key == 'false':
				row.key_strength = output_elem.find('KeyStrength').text

				key_violation_parent_elem = output_elem.find('KeyViolations')

				for key_violation_index, key_violation_child_elem in enumerate(key_violation_parent_elem.iterchildren(), 1):
					if key_violation_index <= 5:
						key_violation_column_value = key_violation_child_elem.find('ColumnValues').find('ColumnValue').text
						key_violation_count = key_violation_child_elem.find('Count').text
						setattr(row, f'key_value_{key_violation_index}', key_violation_column_value)
						setattr(row, f'key_value_count_{key_violation_index}', key_violation_count)
					else:
						pass

		else:
			print(output_elem.tag)

	# when done creating dict of ProfileRows (one per each schema, table, column) ...

	# grid = Grid(...)
	# for key in sorted(rows.keys()):
	# 	row = rows[key]

	# Excel Logic
	environment = 'dev'
	time = datetime.datetime.now()
	file_name = f'''..\output\Data_Profile_Export_{environment}_{time:%Y-%m-%d}.xlsx'''

	# create workbook and worksheets
	workbook = xlsxwriter.Workbook(file_name)
	worksheet1 = workbook.add_worksheet('Profile Overview')
	# header_row = ['Schema Name', 'Table Name', 'Column Name', 'Null Count', 'Min Value', 'Max Value', 'Mean Value'
	# 	, 'Standard Deviation', 'Min Length', 'Max Length', 'Length 1', 'Max Count 1', 'Length 2', 'Max Count 2', 'Length 3'
	# 	, 'Max Count 3', 'Length 4', 'Min Count 3', 'Length 5', 'Min Count 2', 'Length 6', 'Min Count 3']
	header_format = workbook.add_format({'bold': True, 'font_color': 'red'})
	header_format.set_underline()
	worksheet1.write_row(0, 0, row.__dict__.keys(), header_format)
	worksheet1.freeze_panes(1, 0)

	# Start the magic
	for row_index, row in enumerate(rows, 1):
		# print(row_index)
		# print(row)
		# print('hello')
		worksheet1.write_row(row_index, 0, rows[row].__dict__.values())

	# start it up
	workbook.close()
	os.startfile(file_name)
	# pass row to grid for formatting/output
