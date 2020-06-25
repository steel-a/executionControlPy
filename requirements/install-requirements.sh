installPackage(){
    MODULE=$1
    DIR="${HOME}/apps/packages/${MODULE}"
    if [ ! -d "$DIR" ]; then
        git clone http://github.com/steel-a/${MODULE} $DIR

        FILE=${DIR}/requirements/install-requirements.sh
        if test -f "$FILE"; then
            sh $FILE
        fi        
    else echo "Dir already exists: ${DIR}"
    fi
}

installPackage dbpy
