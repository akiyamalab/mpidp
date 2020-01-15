//============================================================================//
//
//  Software Name : MPIDP
//
//  Contact address : Tokyo Institute of Technology, AKIYAMA Lab.
//
//============================================================================//

#ifndef Mpidp_h
#define Mpidp_h 1

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <fstream>
#include <iostream>
#include <sstream>
#include <time.h>
#include <map>
#include <sys/time.h>
#include <sys/stat.h>
#include <mpi.h>

using namespace std;

// JOB management table
typedef struct {
  string  name;    // Job name
  int    exec;    // EXEC
  int    status;    // calculation control flag
  vector<int>  worker;    // WID
  vector<int>  rcode[3];  // 0:END 1:RET 2:FILE
} NameLog;

// JOB control table
typedef struct {
  int           task_id;        // task id in table file
  int           ready;          // ready execution
  int           run;            // during execution
  int           done;           // done execution
  vector<int>  depend;    // job ids (depend)
  vector<int>  depended;  // job ids (depended)
} JobControl;

// Worker management table
typedef struct {
  vector<string> name;    // Job name
  vector<int>    rcode;   // 0: failure 1:success
  int    failure;  // calculation failure counter
  int    run;      // runing rank
} WorkerLog;

class Mpidp
{
 private:
  Mpidp(Mpidp &c){}
  const Mpidp & operator=(const Mpidp &c);
  MPI_Status    _Status;
  string    _Table_file;
  string    _Out_file;
  int      _Out_option;
  int      _Ntry;
  int      _Worker_life;
  string    _Title;
  string    _Param;
  char      _Name[7];
  vector<string>  _Table_list;
  NameLog    *_Namelog;
  WorkerLog    *_Workerlog;
  int      _Csize;
  int      _Psize;
  int      _Ndata;

  map<int,int>    _jobid_map;   // < task id , job id >
  map<int,int>    _taskid_map;  // < job id , task id >
  JobControl    *_JobControl;

 protected:
  virtual string  erase_space(const string &s0,const int ip);
  virtual int    argument(int argc,char *argv[],char **wargv);
  virtual int    argument(int argc,char *argv[],string &main_argv);
  virtual int    for_worker(int &retry,char *ctable,int argc2,char **wargv);
  virtual void    for_worker(int &retry,char *ctable,int &ia,string &argv_joblist);
  virtual string  replace_pattern(const string &pattern,const string &position,const string &option);
  virtual int    count_comma(const string &str);
  virtual void    clear_JobControl();
  virtual int    getNotRunRank(const int &proc);
 public:
  Mpidp() {
#ifdef DEBUG
    cout << "Constructing Mpidp.\n";
#endif
  }
  virtual ~Mpidp() {
#ifdef DEBUG
    cout << "Destructing Mpidp.\n";
#endif
  }
  virtual void    read_table(int argc,char *argv[],int &ntry,ofstream &logout);
  virtual int    master0(const int &nproc);
  virtual int    master(const int &nproc);
  virtual int    master1(const int &nproc);
  virtual void    worker(int &myid,char *hostname,int argc,char *argv[]);
  virtual void    write_table(const int &nproc,ofstream &logout);

  int      _Job_order;
  virtual int           getNextReadyJobID();
  virtual void          resetReadyJobID(int &jobid);
  virtual int           checkDependTaskID(int &jobid);
  virtual void          writeJobControl();
};

#endif
