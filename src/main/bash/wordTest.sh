wordCount(){
    for word in $(cat $1);
    do

    echo "$word: $(grep -c $word $1)";

    done | sort -u
}
add(){
    echo "Total =  `expr $1 + $2`"
}
"$@"
