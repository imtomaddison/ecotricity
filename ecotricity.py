#### Comments ####
'''
Solutions to both challenges found as shown in instructions. 

For Challenge 1:
1. Need to implement additional code to check the period in the mprns csv is within the start/end_time specified in the costs.csv. 
	Works for the given data, however if the period was outside the date range in the other dataframe, it may not work.
	See comments on line 110.
'''
##### End Comments #####


import os
import pandas as pd
import datetime
import calendar


CSD = "2019-07-20"
CED = "2019-10-10"

mprns = pd.read_csv(f"{os.getcwd()}/eco-python-challenge/data/mprns.csv")
#print(mprns)


# Make column headers lowercase.
mprns.columns = mprns.columns.str.lower()

# Replace spaces in the column headers with underscore
mprns.columns = mprns.columns.str.replace(" ", "_")


# Make columns capitalized. Which columns? From data it looks like is just the two below?
mprns['ldz'] = mprns['ldz'].str.upper()
# ExitZone didn't have a space in the csv file - to rename
#mprns.rename(columns={'exitzone':'exit_zone'}, inplace=True) # and then change the strings below.
mprns['exitzone'] = mprns['exitzone'].str.upper()

# Capitalise the meter type data.
mprns['meter_type'] = mprns['meter_type'].str.capitalize()

# Check is working
print(mprns.columns)
print(mprns)


# Function to take a data range, and split that into days of each month. Ie. contract from 1st October to 17th January
# that would return a df with values in the no_days column of 31, 30, 31, 17.
def months_template(start_date, end_date):
	# Assume start_date and end_date are both datetime type. 
	# Get the months between the two dates.
	month = start_date.month
	year = start_date.year

	# Create an empty df with appropriate headers.
	calendar_df = pd.DataFrame({
		'period':[],
		'no_days': [],
		})

	# Loop to add a row for each month between the dates
	while datetime.datetime(year, month, 1).date() <= end_date:
		# Need to make sure to add specific day count for the final month:
		if (year, month) == (end_date.year, end_date.month):
			# Assuming that it is inclusive of the end date - if not just -1 from the value
			calendar_df.loc[len(calendar_df.index)] = [datetime.datetime(year, month, 1), int(end_date.day)]

		# Need to make sure the start month is specific too! - Again assume inclusive, else subtract 1.
		elif (year, month) == (start_date.year, start_date.month):
			calendar_df.loc[len(calendar_df.index)]  = [datetime.datetime(year, month, 1), int(calendar.monthrange(year,month)[1] - start_date.day)]

		# If its not the month the date range ends then we want all days in that month
		else:
			# Generate the first day of that month as a datetime with the number of days in that month. 
			calendar_df.loc[len(calendar_df.index)]  = [datetime.datetime(year, month, 1), int(calendar.monthrange(year,month)[1])]

		# Add 1 to the month and deal with Dec --> Jan
		if month == 12:
			year = year + 1
			month = 1
		else: 
			month = month + 1


	return calendar_df

# Tests to check the calendar df works
x = datetime.datetime(2019,7,19).date()
x_1 = datetime.datetime(2019,10,10).date()
#x_1 = datetime.datetime.now().date()
calendar_df = months_template(x, x_1)
#print(calendar_df.tail(10))


# Need to join the calendar_df and the mprns dataframe. Each row in the calendar_df needs to be linked to each of the rows
# in the mprns dataframe. Ie 2 dates in the calendar, and 2 in the mprns dataframe will be a new df of 4 rows.
pricing_template = calendar_df.merge(mprns, how='cross')
#print(pricing_template_df)
#print(pricing_template_df.columns)

# Reduced df (i.e subset of required data)
pricing_template_subset = pricing_template[['mprn', 'meter_type', 'meter_capacity', 'period', 'no_days']]
print(pricing_template_subset)



# Reading the map costs csv file
map_costs = pd.read_csv(f"{os.getcwd()}/eco-python-challenge/data/map_costs.csv")
print(map_costs.tail(28))

'''
match the right price column to the same appropriate site; 
* The from date and end date columns are set between the right period columns from the dataframe above 
(pricing_template_subset) - Need to add this.
* The meter capacity for a given site is within the min/max capacity columns from map_costs - meter_capacity linked in pd.merge. Check whether this just means check the values in the df?
* The meter type column matches the metertype for a given site in the pricing_template subset - done in pd.merge
'''

def match_map_costs(pricing_template_subset, map_costs):
	# Add the price and unit to the pricing_template_subset dataframe thats appropriate based on meter type and meter capacity.

	# Filter the map costs df to get the appropriate rows based on the meter_type and meter_capacity in the pricing template df and is in date range.
	filtered_map_costs_df = map_costs[(map_costs['meter_type'].isin(pricing_template_subset['meter_type']))]
	print(filtered_map_costs_df)

	# Use merge to find the meter type and meter capacity in both dataframes. 
	new_df = pd.merge(pricing_template_subset, map_costs, on=['meter_type', 'meter_capacity'])
	#print(new_df)
	

	template_df = new_df[['mprn', 'meter_type', 'period', 'no_days', 'price', 'unit']]
	# Amending the dataframe so that it is ordered by period (in date order.)
	template = template_df.sort_values(by=['period'])
	return template




map_rates = match_map_costs(pricing_template_subset, map_costs)
print(map_rates)



# Intermediary step - Multiply the rate by the number of days to get total due for that period
# NB map_rates has prices in pounds. We want it in pence so multiply by 100
map_rates['total_due'] = (map_rates['price'] * map_rates['no_days']) * 100 


# Getting the final dataframe in the desired format. 

# Group by the mprn and then sum the total due divided by the no of days summed, to get the weighted average and the number of days it is over
rate_values = list(map_rates.groupby(by=['mprn', 'meter_type'])['total_due'].sum()/map_rates.groupby(by=['mprn', 'meter_type'])['no_days'].sum())
total_days = list(map_rates.groupby(['mprn', 'meter_type'])['no_days'].sum())
# Use .groups.keys() to access the value we are grouping by
mprn = map_rates.groupby(by=['mprn'])['mprn'].groups.keys()

# Create a final df with the values that we want.
final_df = pd.DataFrame({
	'mprn':mprn,
	'no_days':total_days,
	'rate':rate_values,
	})

print(final_df)
# Same as in the challenge sheet.











##### Challenge 2 #####

DUOS_CATEGORY = {"green": 0, "amber": 1, "red": 2}

# Load all sheets into pandas as separate dataframes
excel_file_df = pd.read_excel(f"{os.getcwd()}/eco-python-challenge/data/ch2.xlsx", sheet_name=None)
#print(excel_file_df)

# Look at excel file and get the sheet we want as being "Annex 1 LV and HV charges".
#print(excel_file_df['Annex 1 LV and HV charges'])

# Inspect this sheet and see each line in excel spreadsheet is line in df. Filter to the lines we want using iloc
excel_sheet = excel_file_df['Annex 1 LV and HV charges'].iloc[2:7]
# Rename the columns the same as in the spreadsheet for simplicity
excel_sheet.columns = ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K']

# As going to manipulate the data, set the first column as the index and transpose

excel_table_df = excel_sheet[['A', 'B', 'C', 'D', 'E']].set_index('A').T
#print(excel_table_df.columns)# to get the column names for next line

# Reformat the table and drop any lines with no data (i.e. all nan)
excel_df = excel_table_df[['Time periods','Monday to Friday \n(Including Bank Holidays)\nAll Year','Saturday and Sunday\nAll Year']].dropna(how='all')
#print(excel_df)

# Split the times in the table based on \n
excel_df['Monday to Friday \n(Including Bank Holidays)\nAll Year'] = excel_df['Monday to Friday \n(Including Bank Holidays)\nAll Year'].str.split('\n')
# This has no effect in this case, but if there was not a single weekend rate it would be needed. (It also makes the loop iteration easier.)
excel_df['Saturday and Sunday\nAll Year'] = excel_df['Saturday and Sunday\nAll Year'].str.split('\n')

# Rename Columns
excel_df.columns  = ['Time Band', 'Weekdays', 'Weekend'] 
# Set the time band as the index
excel_df.set_index('Time Band', inplace=True)
print(excel_df)


# Format like challenge 2 final result
final_df = pd.DataFrame({
	'colour':[],
	'week_time':[],
	'start_time':[],
	'end_time':[],
	})

for index, row in excel_df.iterrows():
	index = index.split(" ")[0].lower()
	colour = DUOS_CATEGORY[index]
	#print(colour)
	# Works as necessary
	
	# Row[0] is weekday timings
	#print(row[0])
	if str(row[0]) == 'nan':
		# If the value is nan then we can ignore as column in excel sheet was blank
		pass

	elif len(row[0]) > 1:
		for item in row[0]:
			start_time, end_time = item.split(' - ')[0], item.split(' - ')[1]
			is_weekday = "True"
			#print(f"Start time is {start_time}")
			#print(f"End time is {end_time}")
			#print(f"Index is {index}")
			#print(f"Is a weekday :{is_weekday}")
			final_df.loc[len(final_df.index)] = [colour, is_weekday, start_time, end_time]

	else:
		print("single time")
		# Split the time to a start time and end time
		start_time, end_time = row[0][0].split(' - ')[0], row[0][0].split(' - ')[1]
		is_weekday = "True"
		#print(f"Start time is {start_time}")
		#print(f"End time is {end_time}")
		#print(f"Index is {index}")
		#print(f"Is a weekday :{is_weekday}")
		final_df.loc[len(final_df.index)] = [colour, is_weekday, start_time, end_time]



	# Row[1] is weekend timings
	#print(f"row is {row[1]}")
	if str(row[1]) == 'nan':
		# If the value is nan then we can ignore as column in excel sheet was blank
		pass

	# Not needed in this case, but included for scalability
	elif len(list(row[1])) > 1:
		for item in row[1]:
			start_time, end_time = item[0].split(' - ')[0], item[0].split(' - ')[1]
			is_weekday = "False"
			#print(f"Start time is {start_time}")
			#print(f"End time is {end_time}")
			#print(f"Index is {index}")
			#print(f"Is a weekday :{is_weekday}")
			final_df.loc[len(final_df.index)] = [colour, is_weekday, start_time, end_time]

	# Case where the single time
	else:
		# Split the time to a start time and end time
		start_time, end_time = row[1][0].split(' - ')[0], row[1][0].split(' - ')[1]
		is_weekday = "False"
		#print(f"Start time is {start_time}")
		#print(f"End time is {end_time}")
		#print(f"Index is {index}")
		#print(f"Is a weekday :{is_weekday}")
		final_df.loc[len(final_df.index)] = [colour, is_weekday, start_time, end_time]


# Rewrite 24:00: endtimes as 00:00: for formatting - colons stop minutes also being replaced.
final_df['end_time'] = final_df['end_time'].str.replace('24:', '00:')
# Ordering is different. But results are the same.
print(final_df)






