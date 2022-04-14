//
// Created by Sai Neeraj Kanuri on 06/04/22.
//

#include <iostream>
#include "../Headers/Transaction.h"

using namespace std;

Transaction::Transaction(string from, string to)
{
    this->from = from;
    this->to = to;
}

string Transaction::getFrom()
{
    return this->from;
}

string Transaction::getTo()
{
    return this->to;
}

void Transaction::setLocalFromId(int localFromId)
{
    this->localFromId = localFromId;
}

void Transaction::setLocalToId(int localToId)
{
    this->localToId = localToId;
}

int Transaction::getLocalFromId()
{
    return this->localFromId;
}

int Transaction::getLocalToId()
{
    return this->localToId;
}
