//
// Created by Sai Neeraj Kanuri on 06/04/22.
//

#ifndef EBFTDA_READER_H
#define EBFTDA_READER_H
#include <vector>
#include <iostream>
#include <fstream>

using namespace std;

class Reader
{
public:
    void init(string filePath);
    void populateNextLine();
    vector<string> getProcessValues();
    void setEofReached();
    bool isEofReached();
    void closeFile();
protected:
private:
    string filePath;
    ifstream inFile;
    string nextLine;
    bool eofReached;
};


#endif //EBFTDA_READER_H
