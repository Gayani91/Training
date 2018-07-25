./wordTest.sh
add(){
    ./wordTest.sh add $1 $2
}
wordCount(){
    ./wordTest.sh wordCount data.text
}
"$@"
