//
// Created by Sai Neeraj Kanuri on 06/04/22.
//

#include <iostream>
#include <fstream>
#include <vector>
#include "../Headers/Reader.h"
#include <string.h>

using namespace std;

void Reader::init(string filePath) {
    this->filePath = filePath;
    this->eofReached = false;
    this->inFile.open(this->filePath);

    if (!this->inFile.is_open()){
        printf("Not a valid inputfile <%s>\n", this->filePath.c_str());
        exit(0);
    }

    this->populateNextLine();
}

void Reader::populateNextLine() {
    if(!getline(this->inFile, this->nextLine)){
        this->setEofReached();
    }
}

void Reader::setEofReached() {
    this->eofReached = true;
}

bool Reader::isEofReached() {
    return this->eofReached;
}

vector<string> Reader::getProcessValues(){
    char delimit[]=" \t\n";

    vector<string> processValues;

    char *tempLine = strdup(this->nextLine.c_str());

    char *token = strtok(tempLine, delimit);
    string symbol = token;
    processValues.push_back(symbol);

    token = strtok(NULL, delimit);
    string block_no = token;
    processValues.push_back(block_no);

    token = strtok(NULL, delimit);
    string tx_no = token;
    processValues.push_back(tx_no);

    token = strtok(NULL, delimit);
    string from = token;
    processValues.push_back(from);

    token = strtok(NULL, delimit);
    string to = token;
    processValues.push_back(to);

    token = strtok(NULL, delimit);
    string value = token;
    processValues.push_back(value);

    return processValues;
}

void Reader::closeFile() {
    this->inFile.close();
}