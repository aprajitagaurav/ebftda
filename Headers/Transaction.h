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
        int getLocalFromId();
        int getLocalToId();
        void setLocalFromId(int localFromId);
        void setLocalToId(int localToId);
    protected:
    private:
        string from;
        string to;
        int localFromId;
        int localToId;
};

#endif //EBFTDA_TRANSACTION_H
