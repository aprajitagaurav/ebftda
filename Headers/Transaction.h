//
// Created by Sai Neeraj Kanuri on 06/04/22.
//

#ifndef EBFTDA_TRANSACTION_H
#define EBFTDA_TRANSACTION_H

using namespace std;

class Transaction
{
    public:
        Transaction(string from, string to);
        string getFrom();
        string getTo();
    protected:
    private:
        string from;
        string to;
};

#endif //EBFTDA_TRANSACTION_H
