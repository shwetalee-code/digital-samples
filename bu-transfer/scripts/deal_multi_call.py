from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import as_completed
import subprocess
import sys
def main(process_list,sparkCode,homePath,sparkSessions,logId):
    print(process_list)
    process_list=process_list.split(",")
    print(process_list)
    print(sparkCode)
    print(homePath)
    Sessions=int(sparkSessions)
    with ThreadPoolExecutor(max_workers = Sessions) as executor:
        results = executor.map(run_spark, process_list)

    for result in results:
        print (result)

def run_spark(process_name):
    print(process_name)
    subprocess.check_call(['bash', sparkCode, process_name, homePath, logId])

if __name__=='__main__':
    process_list=sys.argv[1]
    sparkCode=sys.argv[2]
    homePath=sys.argv[3]
    sparkSessions=sys.argv[4]
    logId=sys.argv[5]
    main(process_list,sparkCode,homePath,sparkSessions,logId)