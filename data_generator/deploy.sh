SUBFOLDER=${1:-""}
FILES_PATH=./data/json${SUBFOLDER:+/$SUBFOLDER}
DEST_PATH=v3${SUBFOLDER:+/$SUBFOLDER}

echo "Uploading files from $FILES_PATH to $DEST_PATH"


az storage blob upload-batch --destination centrality --account-name hobbyplace --destination-path $DEST_PATH --source $FILES_PATH --overwrite