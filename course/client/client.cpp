#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>

#include "bdb/db_cxx.h"

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
DbEnv env(0);
bool envInit = false;
map<string, Db*> table2db;
map<string, int> col2type;  // 0 for int, other for varchar
#define TYPE_INT 0

// Index / Helper / Aux
map<string, string> col2table;
map<string, int> col2offset;
map<string, int> table2size;
map<string, double> col2w;
map<string, Db*> col2idx;
map<Db*, int> idx2offset;  // for indexed column, idx2offset[col2idx[c]] == col2offset[c]

// Output
vector<string> result;

// Alloc
vector<string> token;
unsigned char* bufRecord = NULL;
int bufRecordSize = -1;
unsigned char* bufKey = NULL;
int bufKeySize = -1;

void init()
{
	envInit = true;
	env.open("data", DB_INIT_MPOOL | DB_CREATE, 0);
}
string extract(void* rawData, const string& col)
{
	unsigned char* raw = (unsigned char*)rawData;
	int* offset = (int*)&raw[col2offset[col]];
	return string((const char*)&raw[*offset], *(offset+1) - *(offset));
}
int get_idx_key(Db* sdb, const Dbt* key, const Dbt* data, Dbt* result)
{
	unsigned char* raw = (unsigned char*)(data -> get_data());
	int* offset = (int*)&raw[idx2offset[sdb]];
	result -> set_data(&raw[*offset]);
	result -> set_size(*(offset+1) - *(offset));
	return 0;
}
void create(const string& table, const vector<string>& column,
	const vector<string>& type, const vector<string>& key)
{
	table2col[table] = column;
	table2pkey[table] = key;
	int recordSize = 0;
	for (int i = 0; i < type.size(); i++)
	{
		if (type[i] == "INTEGER")
		{
			col2type[column[i]] = TYPE_INT;
			recordSize += 10;
		}
		else
		{
			int len = -1;
			sscanf(type[i].c_str(), "VARCHAR(%d)", &len);
			col2type[column[i]] = len;
			recordSize += len;
			if (len > bufKeySize)
				bufKeySize = len;
		}
		col2table[column[i]] = table;
		col2offset[column[i]] = i * 4;
	}
	recordSize += (column.size() + 1) * 4;
	if (recordSize > bufRecordSize)
		bufRecordSize = recordSize;
	if (!envInit)
		init();
	Db* db = new Db(&env, 0);
	db -> open(NULL, table.c_str(), NULL, DB_RECNO, DB_CREATE | DB_EXCL, 0);
	table2db[table] = db;
	table2size[table] = 0;
}

void train(const vector<string>& query, const vector<double>& weight)
{
	// some init
	bufRecord = new unsigned char[bufRecordSize];
	bufKey = new unsigned char[bufKeySize];
	// train
	for (int i = 0; i < query.size(); i++)
	{
		splitBySpace(query[i].c_str(), token);
		int iToken = 0;
		if (token[0] == "INSERT")
			continue;
		while (++iToken < token.size() && token[iToken] != "WHERE");
		while (++iToken < token.size())
			if (
				token[iToken] != "AND" && token[iToken] != ";" &&
				token[iToken] != "<" && token[iToken] != ">" && token[iToken] != "=" &&
				token[iToken][0] != '\'' && !('0' <= token[iToken][0] && token[iToken][0] <= '9')
				)
			{
				if (col2w.count(token[iToken]) == 0)
				{
					col2w[token[iToken]] = 0;
					// fprintf(stderr, "To be indexed: %s.\n", token[iToken].c_str());
				}
				col2w[token[iToken]] += weight[i];
			}
	}
	for (map<string, double>::iterator it = col2w.begin(); it != col2w.end(); ++it)
	{
		Db* db = new Db(&env, 0);
		db -> set_flags(DB_DUPSORT);
		// db -> set_bt_compare();  // integer compare later
		db -> open(NULL, (it -> first).c_str(), NULL, DB_BTREE, DB_CREATE | DB_EXCL, 0);
		table2db[col2table[it -> first]] -> associate(NULL, db, get_idx_key, 0);
		col2idx[it -> first] = db;
		idx2offset[db] = col2offset[it -> first];
	}
}

void load(const string& table, const vector<string>& row)
{
	vector<string> col = table2col[table];
	Db* db = table2db[table];
	for (int i = 0; i < row.size(); i++)
	{
		int bufPos = (col.size() + 1) * 4, colId = 0;
		memcpy(&bufRecord[colId * 4], &bufPos, 4);
		for (int j = 0; j < row[i].size(); j++)
			if (row[i][j] != ',')
				bufRecord[bufPos++] = row[i][j];
			else
			{
				colId++;
				memcpy(&bufRecord[colId * 4], &bufPos, 4);
			}
		colId++;
		memcpy(&bufRecord[colId * 4], &bufPos, 4);
		Dbt key, data(bufRecord, bufPos);
		db -> put(NULL, &key, &data, DB_APPEND);  // TODO: bulk put
		table2size[table]++;
	}
}

void preprocess()
{
	// Build index in load, so that INSERT can use it directly.
}

vector<string> tableName;
vector<map<string, int> > colGroup;
vector<vector<Condition*> > where, condJoin;
vector<vector<string> > condJoinCol;
vector<int> tableSize;
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
		int minCond = -1, minCondIdx = -1;
		for (int j = 0; j < where[tid].size() && (where[tid][j] -> op) == Condition::EQ; j++)
			if (
				col2idx.count(where[tid][j] -> lhs) > 0/* &&
				(minCond == -1 || idx[where[tid][j] -> lhs][where[tid][j] -> rhs].size() < minCond)*/
				)
				minCondIdx = j, minCond = 0;
		Dbc* cur = NULL;
		Dbt *key = NULL, data;
		if (minCondIdx != -1)
		{
			Condition* cond = where[tid][minCondIdx];
			col2idx[cond -> lhs] -> cursor(NULL, &cur, DB_CURSOR_BULK);
			memcpy(bufKey, (cond -> rhs).c_str(), (cond -> rhs).size());
			key = new Dbt(bufKey, (cond -> rhs).size());
			// fprintf(stderr, "Using index for %s!\n", (where[tid][minCondIdx] -> lhs).c_str());
		}
		else
		{
			table2db[tableName[tid]] -> cursor(NULL, &cur, DB_CURSOR_BULK);
			key = new Dbt();
		}
		for (
			int ret = cur -> get(key, &data, (minCondIdx != -1)?DB_SET:DB_NEXT);
			ret == 0;
			ret = cur -> get(key, &data, (minCondIdx != -1)?DB_NEXT_DUP:DB_NEXT)
			)
		{
			int lhs, rhs;
			bool pass = true;
			for (int j = 0; j < where[tid].size() && pass; j++)
			{
				if (where[tid][j] -> rhs[0] == '\'')  // VARCHAR - Condition::EQ
					pass = extract(data.get_data(), where[tid][j] -> lhs) == where[tid][j] -> rhs;
				else  // INTEGER
				{
					sscanf(extract(data.get_data(), where[tid][j] -> lhs).c_str(), "%d", &lhs);
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
				row[it -> second] = extract(data.get_data(), it -> first);
			for (int j = 0; j < condJoin[tid].size(); j++)
				condJoin[tid][j] -> rhs = extract(data.get_data(), condJoinCol[tid][j]);
			select(tid + 1);
		}
		cur -> close();
		delete key;
	}
	// indent.resize(indent.size() - 4);
	// fprintf(stderr, "%sselect(%d) end\n", indent.c_str(), tid);
}
vector<string> output;
map<string, int> table;
void execute(const string& sql)
{
	// fprintf(stderr, "execute begin\n");

	splitBySpace(sql.c_str(), token);
	int iToken = 0;

	if (token[0] == "INSERT")
	{
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
		{
			tableName.push_back(token[iToken]);
			tableSize.push_back(table2size[token[iToken]]);
		}
	iTable = tableSize.size();
	// BEGIN Sort table by size
	for (int i = iTable; i >= 2; i--)
	{
		bool ok = true;
		for (int j = 0; j < i - 1; j++)
			if (tableSize[j] > tableSize[j + 1])
			{
				ok = false;
				string ts = tableName[j];
				tableName[j] = tableName[j + 1];
				tableName[j + 1] = ts;
				int ti = tableSize[j];
				tableSize[j] = tableSize[j + 1];
				tableSize[j + 1] = ti;
			}
		if (ok)
			break;
	}
	// END Sort table by size
	for (int i = 0; i < iTable; i++)
		table[tableName[i]] = i;
	colGroup.resize(iTable);
	for (int i = 0; i < output.size(); i++)
		colGroup[table[col2table[output[i]]]][output[i]] = i;
	row.resize(output.size());
	output.clear();

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
	token.clear();
	table.clear();
	// BEGIN Condition table by intelligence
	for (int tid = 0; tid < iTable; tid++)
		for (int i = where[tid].size(); i >= 2; i--)
		{
			bool ok = true;
			for (int j = 0; j < i - 1; j++)
				if ((where[tid][j] -> op) != Condition::EQ && (where[tid][j + 1] -> op) == Condition::EQ)
				{
					ok = false;
					Condition* t = where[tid][j];
					where[tid][j] = where[tid][j + 1];
					where[tid][j + 1] = t;
				}
			if (ok)
				break;
		}
	// BEGIN Condition table by intelligence

	indent.clear();
	select(0);
	row.clear();

	tableName.clear();
	colGroup.clear();
	tableSize.clear();
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
	for (map<string, Db*>::iterator it = col2idx.begin(); it != col2idx.end(); ++it)
		it -> second -> close(0);
	for (map<string, Db*>::iterator it = table2db.begin(); it != table2db.end(); ++it)
		it -> second -> close(0);
	env.close(0);
	delete [] bufRecord;
	delete [] bufKey;
}


