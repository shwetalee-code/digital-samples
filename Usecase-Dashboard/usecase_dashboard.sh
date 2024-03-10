homepath=/home/unix/ops/USECASE_SUMMARY
currentDate=`date +%Y%m%d`
#currentDate="20240130"
currentTimestamp=`date +%Y%m%d_%H%M%S`
#currentTimestamp="20240130_235959"
echo "USECASE,SLA,FREQ,$currentDate" > $homepath/daily_output.txt

for i in `cat $homepath/masterFile_usecase.txt | awk -F "," '{print $1}'`
do
	recentdata=`cat $homepath/raw_data.txt | grep -iw ${1}`
	recentdata_master=`cat $homepath/masterFile_usecase.txt | grep -iw ${1}`
	useCase_name=`echo $recentdata_master | awk -F "," '{print $1}'`
	SLA=`echo $recentdata_master | awk -F "," '{print $2}'` 
	Frequency=`echo $recentdata_master | awk -F "," '{print $3}'`
	category=`echo $recentdata | awk -F "," '{print $8}'`
	echo $useCase_name,$SLA,$Frequency,$category >> $homepath/daily_output.txt
done

### Taking backup
cp $homepath/raw_data.txt $homepath/backup/${currentTimestamp}_raw_data.txt
cp $homepath/masterFile_usecase.txt $homepath/backup/${currentTimestamp}_masterfile_usecase.txt 


### Adding Header to yesterday file ###
NFCNT=`cat $homepath/till_yesterday_data.txt | head -1 | awk -F "," '{print NF}'`
header=`cat $homepath/till_yesterday_data.txt | head -1`
for (( i=6; i<=$NFCNT; i++ ))
do
	headeroutput=`echo $header | awk -v varcnt="$i" -F "," '{print $varcnt}'`
	finalheader=`echo $finalheader","$headeroutput`
	echo $finalheader
done tail -1 > $homepath/yesterday_output.txt


### Appending data to yesterday file in order of master file ###
for i in `cat $homepath/masterFile_usecase.txt | awk -F "," '{print $1}'` 
do 
	wholeLine=`cat $homepath/till yesterday_data.txt | grep -iw ${i}`
	for (( i=6; i<=$NFCNT; i++ ))
	do
		inputfeed=`echo $wholeLine | awk -v varcnt="$i" -F "," '{print $varcnt}'` 
		final=`echo $final","$inputfeed`
		echo $final
	done | tail -1
done >> $homepath/yesterday_output.txt


### Creating the combined intermediate file without the MTD Values ### 
currentFileLineHeader1=`cat $homepath/daily_output.txt | head -1 | awk -F "," '{print $1}'`
currentFileLineHeader2=`cat $homepath/daily_output.txt | head -1 | awk -F "," '{print $2}'`
currentFileLineHeader3=`cat $homepath/daily_output.txt | head -1 | awk -F "," '{print $3}'`
currentFileLineHeader4=`cat $homepath/daily_output.txt | head -1 | awk -F "," '{print $4}'`
yesterdayFileLineHeader=`cat $homepath/yesterday_output.txt | head -1`

combinedHeader=`echo $currentFileLineHeader1","$currentFileLineHeader2","$currentFileLineHeader3",""MTD"",""MTD-DL"","$currentFileLineHeader4$yesterdayFileLineHeader`
echo $combinedHeader | tail -1 > $homepath/combinedoutput_wo_MTD.txt
masterFileCnt=`cat $homepath/daily_output.txt | wc -l`

for (( i=2; i<=$masterFileCnt; i++ ))
do
	currentFileLine1=`cat $homepath/daily_output.txt | head -$i | tail -1 | awk -F "," '{print $1}'`
	currentFileLine2=`cat $homepath/daily_output.txt | head -$i | tail -1 | awk -F "," '{print $2}'`
	currentFileLine3=`cat $homepath/daily_output.txt | head -$i | tail -1 | awk -F "," '{print $3}'`
	currentFileLine4=`cat $homepath/daily_output.txt | head -$i | tail -1 | awk -F "," '{print $4}'`
	yesterdayFileLine=`cat $homepath/yesterday_output.txt | head -$i | tail -1`
	combined=`echo $currentFileLine1","$currentFileLine2","$currentFileLine3","""","""","$currentFileLine4$yesterdayFileLine` 
	echo $combined
done >> $homepath/combined_output_wo_MTD.txt

### Calculating MTD (Average percentage of success till date of the month) ###
MTD_header=`cat $homepath/combined_output_wo_MTD.txt | head -1`
echo $MTD_header > $homepath/combined_output_with_MTD.txt

masterFileCnt=`cat $homepath/daily_output.txt | wc -l`
for (( i=2; i<=$masterFileCnt; i++ ))
do	
	MTDline=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | grep -iwo OK | wc -l`
	MTDDLline=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | grep -iwo DLT | wc -l`


	NFCNT_MTD_tmp=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | awk -F "," '{print NF}'` 
	#NFCNT_blank_tmp=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | grep -o YES | wc -l`
	#NFCNT_MTD=`echo $(($NFCNT_MTD_tmp-4))`
	NFCNT_MTD=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | grep -iwoe OK -iwoe DBA -iwoe DLT -iwoe FIN -iwoe PRX -iwoe SFD -iwoe CBS -iwoe PRM -iwoe SRC -iwoe TGT | wc -l`
	MTDDLline_final=`echo $(($NFCNT_MTD-$MTDDLline))`
	percentValue=100
	MTD_calculated=`echo $(($MTDline*$percentValue))`
	#echo $MTD_calculated
	MTD_calculated_div=`echo $(($MTD_calculated/$NFCNT_MTD))`
	#echo $MTD_calculated_div

	MTDDL_calculated=`echo $(($MTDDLline_final*$percentValue))`
	#echo $MTDDL_calculated
	MTDDL_calculated_div=`echo $(($MTDDL_calculated/$NFCNT_MTD))` 
	#echo $MTDDL_calculated_div
	
	MTDwo1=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | awk -F "," '{print $1}'`
	MTDwo2=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | awk -F "," '{print $2}'`
	MTDwo3=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | awk -F "," '{print $3}'`
	MTDwo6=`cat $homepath/combined_output_wo_MTD.txt | head -$i | tail -1 | awk -F "," '{print $6}'`
	
	MTDyesterdayFileLine=`cat $homepath/yesterday_output.txt | head -$i | tail -1`
	
	MTDcombined=`echo $MTDwo1","$MTDwo2","$MTDwo3","$MTD_calculated_div","$MTDDL_calculated_div","$MTDwo6$MTDyesterdayFileLine`
	echo $MTDcombined
done >> $homepath/combined_output_with_MTD.txt


##### SORTING ######
cat $homepath/combined_output_with_MTD.txt | head -1 > $homepath/combined_output_with_MTD_temp_prev.txt

finalFileCnt=`cat $homepath/combined_output_with_MTD.txt | wc -l`
finalFileCnt_1=`echo $(($finalFileCnt-1))`

####### CHECK #######
cat $homepath/combined_output_with_MTD.txt | tail -$finalFileCnt_1 | sort -t ',' -nb -k4,4 -t ',' -b -k3,3 >> $homepath/combined_output_with_MTD_temp_prev.txt


###### Putting DAILY usecases on top and MONTHLY at the bottom ########
cat $homepath/combined_output_with_MTD_temp_prev.txt | grep -vwe MLY_01 -vwe MLY_21 -vwe MLY_25 -vwe MLY_16 -vwe MLY_26 -vwe MLT_15 -vwe DLY_MLY_16 > $homepath/combined_output_with_MTD_temp.txt
cat $homepath/combined_output_with_MTD_temp_prev.txt | grep -we MLY_01 -we MLY_21 -we MLY_25 -we MLY_16 -we MLY_26 -we MLT_15 -we DLY_MLY_16 >> $homepath/combined_output_with_MTD_temp.txt

cp $homepath/combined_output_with_MTD_temp.txt $homepath/combined_output_with_MTD.txt

cat $homepath/raw_data.txt | grep -v SUCCESS > $homepath/raw_data_publish.txt

####### HTML Convertion ########
cat $homepath/html_scripts/html_table_style.txt > $homepath/html_scripts/usecase_summary.html
sh $homepath/html_scripts/html_conversion_scripts.sh >> $homepath/html_scripts/usecase_summary.html

cat $homepath/html_scripts/html_table_style_daily_data.txt > $homepath/html_scripts/failure_reasons.html
sh $homepath/html_scripts/html_conversion_scripts_daily_data.sh >> $homepath/html_scripts/failure_reasons.html

echo "
Kindly find attached the consolidated Usecase Summary.

Regards,
Team" | mail -a $homepath/html_scripts/usecase_summary.html -a $homepath/html_scripts/failure_reasons.html -s "Usecase Summary - $currentTimestamp" team@mail.com

######## Taking backup of dashboard report #########
cp $homepath/combined_output_with_MTD.txt $homepath/till_yesterday_data.txt
cp $homepath/till_yesterday_data.txt $homepath/backup/${currentTimestamp}_till_yesterday_data.txt