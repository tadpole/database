#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>

#include "bdb/dbstl_vector.h"
using namespace dbstl;

#include "../include/client.h"
#include "../tool/split_csv.h"

using namespace std;

struct Condition
{
	string lhs, rhs;
	enum OP {LT, EQ, GT, NUL};
	OP op;
	Condition() {op = NUL;}
	Condition(const string& lhs, const string& rhs, OP op)
	{
		this -> lhs = lhs;
		this -> rhs = rhs;
		this -> op = op;
	}
	void show() const
	{
		fprintf(stderr, "Condition (%s) (%d) (%s)\n", lhs.c_str(), op, rhs.c_str());
	}
};
void printStr2Int(const map<string, int>& s2i)
{
	for (map<string, int>::const_iterator it = s2i.begin(); it != s2i.end(); ++it)
		fprintf(stderr, "s2i: (%s) => (%d)\n", (it -> first).c_str(), it -> second);
}
void splitBySpace(const char* buf, vector<string>& token)
{
	char temp[65536];
	token.clear();
	for (int i = 0; buf[i]; i++)
	{
		if (buf[i] <= ' ')
			continue;
		int j = 0;
		while (buf[i] > ' ')
			temp[j++] = buf[i++];
		temp[j] = '\0';
		token.push_back(temp);
		i--;
	}
}

// Data
map<string, vector<string> > table2col;
map<string, vector<string> > table2pkey;
map<string, vector<string> > col2data;
map<string, int> col2type;  // 0 for int, other for varchar
#define TYPE_INT 0

// Index / Helper / Aux
map<string, string> col2table;

// Output
vector<string> result;

void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2col[table] = column;
	table2pkey[table] = key;
	for (int i = 0; i < type.size(); i++)
	{
		if (type[i] == "INTEGER")
			col2type[column[i]] = TYPE_INT;
		else
		{
			int len = -1;
			sscanf(type[i].c_str(), "VARCHAR(%d)", &len);
			col2type[column[i]] = len;
		}
		col2table[column[i]] = table;
	}
}

void train(const vector<string>& query, const vector<double>& weight)
{
	// I am too clever; I don't need it.
}

void load(const string& table, const vector<string>& row)
{
	vector<string> col = table2col[table];
	vector<string> token;
	for (int i = 0; i < row.size(); i++)
	{
		split_csv(row[i].c_str(), token);
		for (int j = 0; j < col.size(); j++)
			col2data[col[j]].push_back(token[j]);
	}
}

void preprocess()
{
	// I am too clever; I don't need it.
	db_vector<string> ss;
	ss.push_back("hahaha");
	ss.push_back("hehe");
	fprintf(stderr, "DBTest: %s\n", ss[1].c_str());
}

vector<map<string, int> > colGroup;
vector<vector<Condition*> > where, condJoin;
vector<vector<string> > condJoinCol;
vector<string> row;
string indent;
void select(int tid)
{
	// fprintf(stderr, "%sselect(%d) begin\n", indent.c_str(), tid);
	// indent.append("    ");
	if (tid >= colGroup.size())
	{
		result.push_back(row[0]);
		for (int i = 1; i < row.size(); i++)
			result.back().append("," + row[i]);
	}
	else
	{
		for (int i = 0; i < col2data[colGroup[tid].begin() -> first].size(); i++)
		{
			int lhs, rhs;
			bool pass = true;
			for (int j = 0; j < where[tid].size() && pass; j++)
			{
				if (where[tid][j] -> rhs[0] == '\'')  // VARCHAR - Condition::EQ
					pass = col2data[where[tid][j] -> lhs][i] == where[tid][j] -> rhs;
				else  // INTEGER
				{
					sscanf(col2data[where[tid][j] -> lhs][i].c_str(), "%d", &lhs);
					sscanf((where[tid][j] -> rhs).c_str(), "%d", &rhs);
					if (where[tid][j] -> op == Condition::LT)
						pass = lhs < rhs;
					else if (where[tid][j] -> op == Condition::GT)
						pass = lhs > rhs;
					else  // Condition::EQ
						pass = lhs == rhs;
				}
			}
			if (!pass)
				continue;
			for (map<string, int>::iterator it = colGroup[tid].begin(); it != colGroup[tid].end(); ++it)
				if (it -> second >= 0)
					row[it -> second] = col2data[it -> first][i];
			for (int j = 0; j < condJoin[tid].size(); j++)
				condJoin[tid][j] -> rhs = col2data[condJoinCol[tid][j]][i];
			select(tid + 1);
		}
	}
	// indent.resize(indent.size() - 4);
	// fprintf(stderr, "%sselect(%d) end\n", indent.c_str(), tid);
}
vector<string> output;
map<string, int> table;
void execute(const string& sql)
{
	// fprintf(stderr, "execute begin\n");

	vector<string> token;
	splitBySpace(sql.c_str(), token);
	int iToken = 0;

	if (token[0] == "INSERT")
	{
		exit(0);
		string tableName = token[2];
		for(iToken = 4; iToken < token.size(); iToken += 4)
		{
			output.push_back(token[iToken+1]);
		}
		load(tableName, output);
		output.clear();
		return;
	}

	while (++iToken < token.size() && token[iToken] != "FROM")
		if (token[iToken] != ",")
			output.push_back(token[iToken]);
	int iTable = 0;
	while (++iToken < token.size() && token[iToken] != "WHERE")
		if (token[iToken] != "," && token[iToken] != ";")
			table[token[iToken]] = iTable++;
	colGroup.resize(iTable);
	for (int i = 0; i < output.size(); i++)
		colGroup[table[col2table[output[i]]]][output[i]] = i;
	row.resize(output.size());
	output.clear();
	for (map<string, int>::iterator it = table.begin(); it != table.end(); ++it)
		if (colGroup[it -> second].size() == 0)
			colGroup[it -> second][table2pkey[it -> first][0]] = -1;

	// fprintf(stderr, "from end & where begin\n");

	where.resize(iTable);
	condJoin.resize(iTable);
	condJoinCol.resize(iTable);
	iToken++;
	while (iToken < token.size())
		if (token[iToken] == "AND" || token[iToken] == ";")
			iToken++;
		else
		{
			switch (token[iToken+1][0])
			{
			case '<':
				where[table[col2table[token[iToken]]]].push_back(new Condition(token[iToken], token[iToken+2], Condition::LT));
				break;
			case '>':
				where[table[col2table[token[iToken]]]].push_back(new Condition(token[iToken], token[iToken+2], Condition::GT));
				break;
			case '=':
				if (token[iToken+2][0] == '\'' || ('0' <= token[iToken+2][0] && token[iToken+2][0] <= '9'))
					where[table[col2table[token[iToken]]]].push_back(new Condition(token[iToken], token[iToken+2], Condition::EQ));
				else
				{
					exit(0);
					string colL = token[iToken], colR = token[iToken+2];
					if (table[col2table[colL]] < table[col2table[colR]])
					{
						string t = colL;
						colL = colR;
						colR = t;
					}
					Condition* cond = new Condition(colL, colR, Condition::EQ);
					where[table[col2table[colL]]].push_back(cond);
					condJoin[table[col2table[colR]]].push_back(cond);
					condJoinCol[table[col2table[colR]]].push_back(colR);
				}
				break;
			default:
				where[table[col2table[token[iToken]]]].push_back(new Condition(token[iToken], token[iToken+2], Condition::NUL));
			}
			iToken += 3;
		}
	table.clear();

	indent.clear();
	select(0);
	row.clear();

	colGroup.clear();
	for (int i = 0; i < where.size(); i++)
		for (int j = 0; j < where[i].size(); j++)
			delete where[i][j];
	where.clear();
	condJoin.clear();
	condJoinCol.clear();
}

int next(char *row)
{
	if (result.size() == 0)
		return 0;
	strcpy(row, result.back().c_str());
	result.pop_back();
	// printf("NEXT: (%s)\n", row);
	return 1;
}

void close()
{
	// I have nothing to do.
}


