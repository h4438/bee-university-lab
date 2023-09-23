dvc init
echo "Input drive id"
read drive_id
echo gdrive://$drive_id
dvc remote add -df origin gdrive://$drive_id
