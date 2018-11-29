import requests
import time
import json
import csv
import codecs
import sys
import datetime
sys.stdout = codecs.getwriter('latin-1')(sys.stdout.buffer)

# construct the headers needed to send with the API request including the private API key
api_key = 'redacted'
api_key_header = 'Token {}'.format(api_key)
header = {'Authorization': api_key_header,'Content-Type':'application/json;charset=UTF-8'}

# define the name of the cache file
CACHE_FNAME = 'employer_posted_opps_report.json'

# define a function to run a new report and cache the results of the report in a local .json file
def run_new_report():
    # call the Symplicity CSM API to initiate a new run of the report
    requests.put('https://umichlsa-csm.symplicity.com/api/public/v1/reports/75b087edf2748872f040ce5441a8771a/run',headers=header)

    # wait 5 seconds before continuing to allow the report to run
    time.sleep(5)

    # call the Symplicity CSM API to check the status of the report and make sure it is done running; if it's not, wait 2 seconds longer and then check again
    status = requests.get('https://umichlsa-csm.symplicity.com/api/public/v1/reports/75b087edf2748872f040ce5441a8771a/status',headers=header)
    status_diction = status.json()
    while status_diction['status'] != 'complete':
        time.sleep(2)
        status = requests.get('https://umichlsa-csm.symplicity.com/api/public/v1/reports/75b087edf2748872f040ce5441a8771a/status',headers=header)
        status_diction = status.json()

    # call the Symplicity CSM API to get the .csv contents of the report
    response = requests.get('https://umichlsa-csm.symplicity.com/api/public/v1/reports/75b087edf2748872f040ce5441a8771a/data',headers=header)
    opps = response.json()

    # save the .csv contents in a cache file for future use
    cache_file = open(CACHE_FNAME,'w')
    cache_file.write(json.dumps(opps))
    cache_file.close()

# define a function to get the cached data out of the cache file
def get_cached_data():
        cache_file = open(CACHE_FNAME,'r')
        cache_contents = cache_file.read()
        opps = json.loads(cache_contents)
        cache_file.close()
        return opps

# define a class Opps where each instance is an opportunity/job
class Opps:
    def __init__(self,opps_list):
        self.opp_id = opps_list[0]
        self.employer_id = opps_list[1]
        self.employer_name = opps_list[2]
        self.contact_id = opps_list[4]
        self.contact_name = opps_list[3]
        self.opp_created_date = opps_list[5]
        if len(opp[6])>3:
            self.opp_created_by = opps_list[6]
        else:
            self.opp_created_by = "employer"
        if opps_list[7] == "Yes":
            self.symp_recruit_opp = True
        else:
            self.symp_recruit_opp = False
        if len(opps_list[8])>3:
            self.archive_disposition = opps_list[8]
        else:
            self.archive_disposition = "not archived"
        if opps_list[9] == "Yes":
            self.opp_approved = True
        else:
            self.opp_approved = False

    def __str__(self):
        return "An opportunity with ID {} from {} created on {}".format(self.opp_id,self.employer_name,self.opp_created_date)

    def get_year_created(self):
        return self.opp_created_date.split(', ')[1]

    def get_month_created(self):
        date_list = self.opp_created_date.split(', ')
        date_list = date_list[0].split(" ")+date_list[1:]
        return date_list[0]

def get_opps_in_year(list_of_opp_instances,year_num,filter_by_recruit = "all",exclude_unapproved = True,filter_by_posted_by = "all"):
    # check a list of instances of Opps given a year in format "yyyy" and return the number of opportunities posted in that year
    # filter_by_recruit valid values are "all","recruit only", and "non-recruit only"
    # filter_by_posted_by valid values are "all","employer only","staff only"
    count_of_opps = 0
    for opp in list_of_opp_instances:
        if len(opp.opp_created_date)>3:
            if filter_by_recruit == "recruit only":
                if opp.symp_recruit_opp == False:
                    continue
            elif filter_by_recruit == "non-recruit only":
                if opp.symp_recruit_opp == True:
                    continue
            if exclude_unapproved == True:
                if opp.opp_approved == False:
                    continue
            if filter_by_posted_by == "employer only":
                if opp.opp_created_by != "employer":
                    continue
            elif filter_by_recruit == "staff only":
                if opp.opp_created_by == "employer":
                    continue
            if opp.get_year_created() == year_num:
                count_of_opps +=1
    return count_of_opps

def get_opps_in_month(list_of_opp_instances,month_str,year_num,filter_by_recruit = "all",exclude_unapproved = True,filter_by_posted_by = "all"):
    # check a list of instances of Opps given a month in format "m" (ex. 3,10,12) year in format "yyyy" and return the number of opportunities posted in that month of that year
    # filter_by_recruit valid values are "all","recruit only", and "non-recruit only"
    # filter_by_posted_by valid values are "all","employer only","staff only"
    count_of_opps = 0
    for opp in list_of_opp_instances:
        if len(opp.opp_created_date)>3:
            if filter_by_recruit == "recruit only":
                if opp.symp_recruit_opp == False:
                    continue
            elif filter_by_recruit == "non-recruit only":
                if opp.symp_recruit_opp == True:
                    continue
            if exclude_unapproved == True:
                if opp.opp_approved == False:
                    continue
            if filter_by_posted_by == "employer only":
                if opp.opp_created_by != "employer":
                    continue
            elif filter_by_recruit == "staff only":
                if opp.opp_created_by == "employer":
                    continue
            if opp.get_year_created() == year_num:
                if opp.get_month_created() == month_str:
                    count_of_opps +=1
    return count_of_opps

# call the Symplicity CSM API to check when the report with ID 75b087edf2748872f040ce5441a8771a named Employer-Created Postings for Ben was last run; if it was today, use cached data if it exists; if not, run the report and get new data
status = requests.get('https://umichlsa-csm.symplicity.com/api/public/v1/reports/75b087edf2748872f040ce5441a8771a/status',headers=header)
status_diction = status.json()
today = datetime.date.today()
status_date_list = status_diction['created'].split('-')
status_date_list = status_date_list[0:2]+status_date_list[2].split('T')
status_date_list = status_date_list[0:3]
new_status_date_list = []
for numstring in status_date_list:
    new_status_date_list.append(int(numstring))
if new_status_date_list[0]==today.year and new_status_date_list[1]==today.month and new_status_date_list[2]==today.day:
    try:
        opps = get_cached_data()
    except:
        run_new_report()
        opps = get_cached_data()
else:
    run_new_report()
    opps = get_cached_data()

# use the list returned by the API to create a list of instances of Opps where each item in the list is one Opp from the API response
opps_list = []
for opp in opps[1:]:
    opps_list.append(Opps(opp))

# create a dictionary where the first-level keys are 4-digit year strings and the values are the number of Symplicity Recruit jobs posted in that year by employers, the number of non-Symplicity Recruit jobs posted in that year by employers, and the total number of jobs posted in that year by employers
employer_posted_opps_by_year = {}
opps_years = []
for opp in opps_list:
    if len(opp.opp_created_date)>3:
        if opp.get_year_created() not in opps_years:
            opps_years.append(opp.get_year_created())
opps_years.sort()

for year in opps_years:
    employer_posted_opps_by_year[year] = {}
    employer_posted_opps_by_year[year]["recruit only"] = get_opps_in_year(opps_list,year,filter_by_recruit = "recruit only",filter_by_posted_by = "employer only")
    employer_posted_opps_by_year[year]["non-recruit only"] = get_opps_in_year(opps_list,year,filter_by_recruit = "non-recruit only",filter_by_posted_by = "employer only")
    employer_posted_opps_by_year[year]["total"] = get_opps_in_year(opps_list,year,filter_by_posted_by = "employer only")

# export the dictionary employer_posted_opps_by_year to a csv
with open("employer_posted_opps_by_year.csv","w",encoding="latin-1",newline = '') as employer_posted_opps_by_year_csv:
    yearwriter = csv.writer(employer_posted_opps_by_year_csv)
    yearwriter.writerow(["Year","Number of Symplicity Recruit Opportunities in Year","Number of Non-Symplicity Recruit Opportunities in Year","Number of Total Opportunities in Year"])
    for year in opps_years:
        yearwriter.writerow([year,employer_posted_opps_by_year[year]["recruit only"],employer_posted_opps_by_year[year]["non-recruit only"],employer_posted_opps_by_year[year]["total"]])

# create a dictionary where the first-level keys are 4-digit month/year strings and the values are the number of Symplicity Recruit jobs posted in that month/year by employers, the number of non-Symplicity Recruit jobs posted in that month/year by employers, and the total number of jobs posted in that month/year by employers
employer_posted_opps_by_month = {}
months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
opps_months = []
for year in opps_years:
    for month in months:
        opps_months.append(" ".join([month,year]))
for month in opps_months:
    employer_posted_opps_by_month[month] = {}
    employer_posted_opps_by_month[month]["recruit only"] = get_opps_in_month(opps_list,month.split(" ")[0],month.split(" ")[1],filter_by_recruit = "recruit only",filter_by_posted_by = "employer only")
    employer_posted_opps_by_month[month]["non-recruit only"] = get_opps_in_month(opps_list,month.split(" ")[0],month.split(" ")[1],filter_by_recruit = "non-recruit only",filter_by_posted_by = "employer only")
    employer_posted_opps_by_month[month]["total"] = get_opps_in_month(opps_list,month.split(" ")[0],month.split(" ")[1],filter_by_posted_by = "employer only")

# export the dictionary employer_posted_opps_by_year to a csv
with open("employer_posted_opps_by_month.csv","w",encoding="latin-1",newline = '') as employer_posted_opps_by_month_csv:
    monthwriter = csv.writer(employer_posted_opps_by_month_csv)
    monthwriter.writerow(["Month","Number of Symplicity Recruit Opportunities in Month","Number of Non-Symplicity Recruit Opportunities in Month","Number of Total Opportunities in Month"])
    for month in opps_months:
        if employer_posted_opps_by_month[month]["total"] > 0:
            monthwriter.writerow([month,employer_posted_opps_by_month[month]["recruit only"],employer_posted_opps_by_month[month]["non-recruit only"],employer_posted_opps_by_month[month]["total"]])
