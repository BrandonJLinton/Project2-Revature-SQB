### Group 2: Brandon Linton, Syed Rizvi, Quan Vu

# Project2-Revature-SQB

#Project Proposal
Ever wondered which city of the United States has most ridiculous drivers or which roads to avoid
if you happen to live in one of those cities (sorry). Buckle up as we show our traffic report
analysis for three cities namely New York City, Houston, and Philadelphia. The analysis is broken
into two parts, first is looking at total counts and average of traffic incidents excluding accidents
of each city over different timelines (Entire Time, Weekdays, Weekends, and Rush hour) then 
compared to each city the second part focuses on purely accidents. For each city, the location, 
and the road with highest occurrence of accidents will be shown. Also comparing each city with average 
and amount of accidents over the same timeline. Lastly an example of real time streaming on a business 
day to perform live analysis!

#Presentations
	• Bring a simple slide deck providing an overview of your results. You should present your results, a high level overview of the process used to achieve those results, and any assumptions and simplifications you made on the way to those results.
	• I may ask you to run an analysis on the day of the presentation, so be prepared to do so.
	• We'll have 20 minutes per group, so make sure your presentation can be covered in that time, focusing on the parts of your analysis you find most interesting.
	• Include a link to your github repository at the end of your slides

#Technologies
	• Apache Spark
	• Spark SQL
	• YARN
	• HDFS and/or S3
	• Scala 2.12.10
	• Git + GitHub

#Datasource: Twitter: @TotalTrafficNYC, @TotalTrafficPHL, @TotalTrafficHOU

#Assumptions
	• Entire Time: 11/09/2020-11/21/2020

	• Rush Hour: Morning 7:00-10:00 AM and Evening: 4:00-7:00 PM

	• Business day: 8:00-5:00 PM

	• Accident related keyword: 
		• Overturned Vehicle 
		• Closed due to Accident
		• Accident

	• Distinct Accidents cannot have matching:
		• Accident Occurrence Road (which road the accident occurred)
		• Detailed Accident Location (Place nearby accident may even be a nearby road)

	• Incident related keyword (Excluding Accidents):
		• Bridge Closed
		• Ramp Restriction
		• Off-Ramp Blocked
		• Disable Vehicle

#Project Traffic Report Breakdown 

Brandon:
#Non-Accident:
	1.Types of Incidents (Excluding Accidents)

		Non-Accident: NYC
			1. Count and Average mentions for each type of Incident (Timeline: Entire Time)
			2. Count and Average of Total mentions for each type of Incident (Timeline: Weekdays & Weekends)
			3. Count and Average of Total mentions for each type of Incident (Timeline: Rush-Hour)


		Non-Accident: PHL
			1. Count and Average mentions for each type of Incident (Timeline: Entire Time)
			2. Count and Average mentions for each type of Incident (Timeline: Weekdays & Weekends)
			3. Count and Average of Total mentions for each type of Incident (Timeline: Rush-Hour)

		Non-Accident: HOU
			1. Count and Average of Total mentions for each type of Incident (Timeline: Entire Time)
			2. Count and Average of Total mentions for each type of Incident (Timeline: Weekdays & Weekends)
			3. Count and Average of Total mentions for each type of Incident (Timeline: Rush-Hour)

	2. Compare the results of NYC, PHL, HOU (Timeline: Entire Time)
	3. Compare the results of NYC, PHL, HOU (Timeline: Weekdays & Weekends)
	4. Compare the results of NYC, PHL, HOU (Timeline: Rush-Hour)

Quan:	
#Business day (8-5) stream of NYC, PHL, HOU:
		1. The General Location of where most accidents occur in each city. 
		2. Which of these cities had the highest number of accidents?

Syed:	
#Accident: These will be Distinct Accidents (not including updates) Repeat for PHL, HOU
	1. The General Location of where most accidents occur (Timeline: Entire Time)
	2. The General Location of where most accidents occur (Timeline: Weekdays & Weekends)
	3. The General Location of where most accidents occur (Timeline: Rush-Hour)
	4. The Road of where most accidents occur (Timeline: Entire Time)
	5. The Road of where most accidents occur (Timeline: Weekdays & Weekends)
	6. The Road of where most accidents occur (Timeline: Rush Hour)

Syed:
#Accident: NYC, PHL, HOU Comparison:
	1.Which of these cities had the highest number of accidents and average (Timeline: Entire Time)
	2.Which of these cities had the highest number of accidents and average (Timeline: Weekdays & Weekends)
	3.Which of these cities had the highest number of accidents and average (Timeline: Rush Hour)


