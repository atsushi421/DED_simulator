#!/bin/bash


### initialize option variables
TARGET_DIR=""


### parse command options
OPT=`getopt -o d: -l dir: -- "$@"`


if [ $? != 0 ] ; then
    echo "[Error] Option parsing processing is failed." 1>&2
    show_usage
    exit 1
fi

eval set -- "$OPT"

while true
do
    case $1 in
    -d | --dir)
        TARGET_DIR="$2"
        shift 2
        ;;
    --)
        shift
        break
        ;;
    esac
done


### crop
PDF_FILES=$( find ${TARGET_DIR} -name "*.pdf" )
for pdf_file_path in ${PDF_FILES}
do
    pdfcrop ${pdf_file_path}
    rm ${pdf_file_path}
    filename=$(basename ${pdf_file_path} .pdf)
    mv "${TARGET_DIR}/${filename}-crop.pdf" ${pdf_file_path}
done
