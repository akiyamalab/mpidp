//============================================================================/
//
//  Software Name : MPIDP
//
//  Contact address : Tokyo Institute of Technology, AKIYAMA Lab.
//
//============================================================================//

#include "mpidp.h"

#define VERSION "1.1.0"
#define LASTUPDATED "2014/02/19"

#ifndef SYSTEMCALL
int application(int argc,char *argv[]);
#endif

//============================================================================//
int main(int argc,char *argv[])
//============================================================================//
{
  Mpidp mpidp;
  ofstream logout;        // log file stream
  double stime, etime;
  int nproc, myid, resultlen;    // for MPI parameters
  char hostname[MPI_MAX_PROCESSOR_NAME];

  // Preparation using MPI
  MPI_Init(&argc,&argv);
  stime = MPI_Wtime();
  MPI_Comm_size(MPI_COMM_WORLD,&nproc);
  MPI_Comm_rank(MPI_COMM_WORLD,&myid);
  MPI_Get_processor_name(hostname,&resultlen);
  char *hostall = new char [nproc*MPI_MAX_PROCESSOR_NAME];
  MPI_Gather(hostname,MPI_MAX_PROCESSOR_NAME,MPI_CHAR,
       hostall,MPI_MAX_PROCESSOR_NAME,MPI_CHAR, 0,MPI_COMM_WORLD);

  // for master process
  if( myid == 0 ){
    string log_file = "./mpidp.log";    // Default log file name
    for( int i = 1 ; i < argc ; i++ ) {
      if( !strncmp(argv[i],"-lg",3) ) {
        log_file = argv[++i];
      }
    }

    logout.open(log_file.c_str());
    if( !logout ) {
      cerr << "[ERROR] Log file [" << log_file << "] was not opened!!" << endl;
      MPI_Abort(MPI_COMM_WORLD,1);
      exit(1);
    }

    logout << " MPIDP ver. "<< VERSION << endl;
    logout << "      mpidp@bi.cs.titech.ac.jp   last updated: " << LASTUPDATED << endl << endl;
    logout << "#RANK = " << nproc << endl;

    int nprocess = 1;        // # of processes in one core
    string *shost = new string[nproc];    // Hostname
    shost[0] = &hostall[0];
    for( int i = 1 ; i < nproc ; i++ ) {
      shost[i] = &hostall[i*MPI_MAX_PROCESSOR_NAME];
      if( shost[i] == shost[0] ) {
        nprocess ++;
      }
      else {
        break;
      }
    }
    logout << "#Node = " << nproc/nprocess << " (#RANK/Node = " << nprocess << ")" << endl;

    logout << "\n used nodes list(id) :";
    for( int i = 0 ; i < nproc ; i++ ) {
      if( i % 5 == 0 ) {
        logout << endl;
      }
      logout << "  " << &hostall[i*MPI_MAX_PROCESSOR_NAME] << "(" << i << ")";
    }
    logout << endl << endl;
    logout.flush();
    delete [] shost;
  }
  delete [] hostall;

  if( myid == 0 ) {      // for master
    int ntry;        // Upper limit of the number of retries
    int eflag;        // return flag

    // Read command options and JOB list
    mpidp.read_table(argc,argv,ntry,logout);

    if( ntry == 0 ) {      // case of NO retry
      if( mpidp._Job_order ) {
        eflag = mpidp.master1(nproc);  // = 0 (MPI_Finalize) or 1 (MPI_Abort)
      }
      else {
        eflag = mpidp.master0(nproc);  // = 0 (MPI_Finalize) or 1 (MPI_Abort)
      }
    }
    else {
      eflag = mpidp.master(nproc);  // = 1 (MPI_Abort)
    }

    mpidp.write_table(nproc,logout);  // write JOB and Workers report

    if( eflag ) {
      etime = MPI_Wtime();
      logout << "\nElapsed time  = " << etime - stime << " sec." << endl;
      MPI_Abort(MPI_COMM_WORLD,1);
    }
  }
  else {        // for workers
    mpidp.worker(myid,hostname,argc,argv);
  }

  etime = MPI_Wtime();

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();

  if( !myid ) {
    logout << "\nElapsed time  = " << etime - stime << " sec." << endl;
  }

  return 0;
}

//============================================================================//
void Mpidp::read_table(int argc,char *argv[],int &ntry,ofstream &logout)
// Read command options and JOB list
//============================================================================//
{
  int csize = 0;
  int ndata = 0;
  string table;

  _Title = "MPIDP ";
  _Title += VERSION;      // TITLE (initialization)
  _Param = "MPIDP";      // PARAM data (initialization)
  _Psize = _Param.size() + 1;
  _Out_option = 0;
  _Ntry = 0;        // Number retrying limit
  _Worker_life = 3;      // Worker life (default=3)
  _Job_order = 0;                       // Job ordering mode

  // for MPIDP options
  for( int i = 1 ; i < argc ; i++ ) {
    if( !strncmp(argv[i],"-tb",3) ) {
      _Table_file = argv[++i];
      logout << "Table file    : -tb " << _Table_file << endl;
    }
    else if( !strncmp(argv[i],"-ot",3) ) {
      _Out_option = atoi(argv[++i]);
      logout << "Output option : -ot " << _Out_option << endl;
    }
    else if( !strncmp(argv[i],"-rt",3) ) {
      _Ntry = atoi(argv[++i]);
      logout << "Retry option  : -rt " << _Ntry << endl;
    }
    else if( !strncmp(argv[i],"-wl",3) ) {
      _Worker_life = atoi(argv[++i]);
      logout << "Worker life   : -wl " << _Worker_life << endl;
    }
    else if( !strncmp(argv[i],"-jo",3) ) {
      _Job_order = atoi(argv[++i]);
      logout << "JOB Order     : -jo " << _Job_order << endl;
    }
    else if( !strncmp(argv[i],"-pg",3) ) {
      logout << "Program name  : -pg " << argv[++i] << endl;
    }
    else if( !strncmp(argv[i],"-lg",3) ) {
      logout << "Log file      : -lg " << argv[++i] << endl;
    }
  }

  // Check: Option is correct?
  if(_Job_order != 0 && _Job_order != 1) {
    cerr << "[ERROR] -jo : 0 or 1." << endl;
    MPI_Abort(MPI_COMM_WORLD,1);
    exit(1);
  }
  if(_Job_order == 1 && _Ntry > 0) {
    cerr << "[ERROR] -rt > 0 & -jo 1 is impossible mode." << endl;
    MPI_Abort(MPI_COMM_WORLD,1);
    exit(1);
  }

  // for other(application's) options
  int oflag = 0;
  for( int i = 1 ; i < argc ; i++ ) {
    if( !strncmp(argv[i],"-tb",3) ||
        !strncmp(argv[i],"-ot",3) ||
        !strncmp(argv[i],"-rt",3) ||
        !strncmp(argv[i],"-wl",3) ||
        !strncmp(argv[i],"-jo",3) ||
        !strncmp(argv[i],"-pg",3) ||
        !strncmp(argv[i],"-lg",3) ) {
      i++;
    }
    else {
      if( oflag == 0 ) {
        logout << "Other options :";
        oflag = 1;
      }
      logout << " " << argv[i];
    }
  }

  if( oflag == 1 ) {
    logout << endl;
  }
  logout << endl;

  // open JOB list file
  ifstream Input(_Table_file.c_str(),ios::in);
  if( !Input ) {
    cerr << "[ERROR] Table file [" << _Table_file << "] was not opened!!" << endl;
    MPI_Abort(MPI_COMM_WORLD,1);
    exit(1);
  }

  // read JOB list file
  while(1) {
    if( !getline(Input,table) ) break;
    if( !_Job_order ) {
      table = erase_space(table,7);
    }

    if( !strncmp(table.c_str(),"TITLE=",6) ||
        !strncmp(table.c_str(),"Title=",6) ||
        !strncmp(table.c_str(),"title=",6) ) {
      _Title = table.substr(6);
      logout << "TITLE=" << _Title << endl;
    }
    else if( !strncmp(table.c_str(),"PARAM=",6) ||
             !strncmp(table.c_str(),"Param=",6) ||
             !strncmp(table.c_str(),"param=",6) ) {
      _Param = table.substr(6);
      _Psize = _Param.size() + 1;
      logout << "PARAM=" << _Param << endl;
    }
    else {
      // JOB ORDER mode
      if( _Job_order ) {
        size_t i = table.rfind('\t', table.length());
        if (i != string::npos) {
          table = table.substr(i+1, table.length() - i);
        }
      }

      _Table_list.push_back(table);

      if( _Table_list.back().size() > csize ) {
        csize = _Table_list.back().size();
      }
    }
  }

  logout << endl;

  // JOB ORDER mode : make _JobControl[]
  if( _Job_order ) {
    // read JOB list again
    Input.clear();
    Input.seekg(0, std::ios::beg); // rewind

    int tbsize = _Table_list.size();
    _JobControl = new JobControl[tbsize];
    clear_JobControl();
    _jobid_map.clear();
    _taskid_map.clear();

    int ntable = 0;
    while(1) {
      if( !getline(Input,table) ) break;

      if( !strncmp(table.c_str(),"TITLE=",6) ||
          !strncmp(table.c_str(),"Title=",6) ||
          !strncmp(table.c_str(),"title=",6) ) {
      }
      else {
        size_t i = table.rfind('\t', table.length());

        string dep_id = "";
        string id = "";
        if (i != string::npos) {
          dep_id = table.substr(0, i);
          table = table.substr(i+1, table.length() - i);

          i = dep_id.rfind('\t', dep_id.length());
          if (i != string::npos) {
            id = dep_id.substr(0, i);
            dep_id = dep_id.substr(i+1, dep_id.length() - i);

            // save _JobControl[].task_id
            _JobControl[ntable].task_id = std::atoi(id.c_str());

            // save _jobid_map < task id , job id >
            _jobid_map.insert(pair<int,int>(std::atoi(id.c_str()),ntable));
            // save _taskid_map < job id , task id >
            _taskid_map.insert(pair<int,int>(ntable,std::atoi(id.c_str())));

            if (dep_id == "") {
              _JobControl[ntable].ready = 1;
            }
            else {
              int ndepend = count_comma(dep_id) + 1;
              
              // save _JobControl[].depend
              if( ndepend > 0 ) {
                if( ndepend == 1 ) {
                  int task_id = atoi(dep_id.c_str());
                  _JobControl[ntable].depend.push_back(task_id);
                }
                else {
                  for( int i = 0; i < ndepend - 1; i++ ) {
                    size_t j = dep_id.rfind(',', dep_id.length());
                    if (j != string::npos) {
                      string depend0 = dep_id.substr(j+1, dep_id.length() - j);
                      dep_id = dep_id.substr(0, j);
                      int task_id = atoi(depend0.c_str());
                      _JobControl[ntable].depend.push_back(task_id);

                      // last of loop
                      if( i == ndepend - 2) {
                        task_id = atoi(dep_id.c_str());
                        _JobControl[ntable].depend.push_back(task_id);
                      }
                    }
                  }
                }
              }
            }
          }
          else {
            cout << "ERROR: JOB table format" << endl;
            exit(1);
          }
        }
        else {
          cout << "ERROR: JOB table format" << endl;
          exit(1);
        }
        ntable++;
      }
    }

    ntable = 0;
    while(1) {
      if (ntable == tbsize) break;

      // save _JobControl[].depended
      for(int j = 0; j < _JobControl[ntable].depend.size() ; j++ ) {
        int task_id = _JobControl[ntable].depend[j];
        int job_id = _jobid_map[task_id]; // task id -> job id
        _JobControl[job_id].depended.push_back(_taskid_map[ntable]);
      }
      ntable++;
    }
#ifdef DEBUG_LOG
    writeJobControl();
#endif
  }

  Input.close();
  _Csize = csize + 7;

  for( int i = 0 ; i < _Table_list[0].size() ; i++ ) {
    if( _Table_list[0].substr(i,1) == "\t" ) {
      ndata ++;
    }
  }

  // According to _Name data
  _Ndata = ndata + 2;

  ntry = _Ntry;

  return;
}

//============================================================================//
string Mpidp::erase_space(const string &s0,const int ip)
// Deletion of spaces
//============================================================================//
{
  int     n;
  string  s1;

  s1 = s0;

  while(1) {
    n = s1.find(" ");

    if( n == std::string::npos ) {
      break;
    }
    else if( n < ip ) {
      s1.erase(n,1);
    }
    else {
      break;
    }
  }

  return s1;
}

//============================================================================//
int Mpidp::master0(const int &nproc)
// master process for NO retry
//============================================================================//
{
  int wid;        // Worker id
  int retry = 0;      // Retry counter
  int ir[2];        // 0:RET and 1:FILE flags 
  int tbsize = _Table_list.size();
  char end_tb_flag[] = "EndOfTable";  // Table list END flag
  char *param = new char[_Psize];  // for PARAM data

  // for Table list
  char **ctable = new char*[tbsize];
  for( int i = 0 ; i < tbsize ; i++ ) {
    ctable[i] = new char[_Csize];
  }

  strcpy(param,_Param.c_str());

  // The calculation condition is sent to workers. 
  for( int i = 1 ; i < nproc ; i++ ) {
    MPI_Send(&_Psize,1,MPI_INT,i,100,MPI_COMM_WORLD);
    MPI_Send(param,_Psize,MPI_CHAR,i,200,MPI_COMM_WORLD);
    MPI_Send(&_Csize,1,MPI_INT,i,300,MPI_COMM_WORLD);
    MPI_Send(&_Ndata,1,MPI_INT,i,400,MPI_COMM_WORLD);
    MPI_Send(&_Out_option,1,MPI_INT,i,420,MPI_COMM_WORLD);
  }

  _Namelog = new NameLog[tbsize];
  _Workerlog = new WorkerLog[nproc];

  for( int i = 0 ; i < tbsize ; i++ ) {
    sprintf(_Name,"%05d\t\0",i+1);  // Event number
    strcpy(ctable[i],_Name);
    strcat(ctable[i],_Table_list[i].c_str());
    _Namelog[i].name = _Name;
    _Namelog[i].exec = 1;    // EXEC flag = 1 (fixed)

    _Namelog[i].rcode[0].push_back(0);
    _Namelog[i].rcode[1].push_back(-1);
    _Namelog[i].rcode[2].push_back(0);

    if( i < nproc-1 ) {
      _Namelog[i].worker.push_back(i+1);
      _Workerlog[i+1].name.push_back(_Name);
      _Workerlog[i+1].rcode.push_back(0);
      MPI_Send(&retry,1,MPI_INT,i+1,450,MPI_COMM_WORLD);
      MPI_Send(ctable[i],_Csize,MPI_CHAR,i+1,500,MPI_COMM_WORLD);
    }
    else {
      MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
      MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);
      int nameno = atoi(_Workerlog[wid].name.back().c_str());
      _Namelog[nameno-1].rcode[0][0] = 1;
      _Namelog[nameno-1].rcode[1][0] = ir[0];
      _Namelog[nameno-1].rcode[2][0] = ir[1];

      _Workerlog[wid].rcode.back() = 1;

      _Namelog[i].worker.push_back(wid);
      _Workerlog[wid].name.push_back(_Name);
      _Workerlog[wid].rcode.push_back(0);
      MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
      MPI_Send(ctable[i],_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);
    }
  }

  for( int i = 0 ; i < min(nproc-1,tbsize) ; i++ ) {
    MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
    MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);
    int nameno = atoi(_Workerlog[wid].name.back().c_str());
    _Namelog[nameno-1].rcode[0][0] = 1;
    _Namelog[nameno-1].rcode[1][0] = ir[0];
    _Namelog[nameno-1].rcode[2][0] = ir[1];
    _Workerlog[wid].rcode.back() = 1;
    MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
    MPI_Send(end_tb_flag,_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);
  }

  for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
  delete [] ctable;
  delete [] param;

  if( tbsize < nproc-1 ) {
    return 1;
  }
  else {
    return 0;
  }
}

//============================================================================//
int Mpidp::master(const int &nproc)
// master process (which can respond at the time of an obstacle)
//============================================================================//
{
  int wid;        // Worker id
  int retry = 0;      // Retry counter
  int ir[2];        // 0:RET and 1:FILE flags 
  int remain_workers = nproc - 1;  // Effective workers
  int tbsize = _Table_list.size();
  char end_tb_flag[] = "EndOfTable";  // Table list END flag
  char *param = new char[_Psize];  // for PARAM data

  // for Table list
  char **ctable = new char*[tbsize];
  for( int i = 0 ; i < tbsize ; i++ ) {
    ctable[i] = new char[_Csize];
  }

  strcpy(param,_Param.c_str());

  // The calculation condition is sent to workers. 
  for( int i = 1 ; i < nproc ; i++ ) {
    MPI_Send(&_Psize,1,MPI_INT,i,100,MPI_COMM_WORLD);
    MPI_Send(param,_Psize,MPI_CHAR,i,200,MPI_COMM_WORLD);
    MPI_Send(&_Csize,1,MPI_INT,i,300,MPI_COMM_WORLD);
    MPI_Send(&_Ndata,1,MPI_INT,i,400,MPI_COMM_WORLD);
    MPI_Send(&_Out_option,1,MPI_INT,i,420,MPI_COMM_WORLD);
  }

  _Namelog = new NameLog[tbsize];
  _Workerlog = new WorkerLog[nproc];

  // JOB management table initialization
  for( int i = 0 ; i < tbsize ; i++ ) {
    sprintf(_Name,"%05d\t\0",i+1);  // Event number
    strcpy(ctable[i],_Name);
    strcat(ctable[i],_Table_list[i].c_str());
    _Namelog[i].name = _Name;
    _Namelog[i].exec = 0;    // EXEC flag increment(=retry)
    _Namelog[i].status = 0;    // calculation control flag
  }

  // Worker management table initialization
  for( int i = 1 ; i < nproc ; i++ ) {
    _Workerlog[i].failure = 0;    // Initialize failure counter
  }

  int itry = 0;
  int jstart = 0;
  int init_counter = 0;      // initial loop counter

  while(1) {
    bool table_remains = false;    // Remains of table check flag
    int ic;

    for( ic = itry ; ic < _Ntry+1 ; ic++ ) {
      for( int j = jstart ; j < tbsize ; j++ ) {
        if( _Namelog[j].exec == ic && _Namelog[j].status < _Ntry+1 ) {
          table_remains = true;
          if( init_counter < nproc-1 ) {
            wid = ++init_counter;
          }

          retry = _Namelog[j].exec;
          _Namelog[j].exec ++;

          _Namelog[j].rcode[0].push_back(0);
          _Namelog[j].rcode[1].push_back(-1);
          _Namelog[j].rcode[2].push_back(0);

          _Namelog[j].worker.push_back(wid);
          _Workerlog[wid].name.push_back(_Namelog[j].name);
          _Workerlog[wid].rcode.push_back(0);
          MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
          MPI_Send(ctable[j],_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);

          if( init_counter == nproc-1 ) {
            break;
          }
        }
      }

      if( table_remains ) {
        break;
      }
    }

    if( !table_remains ) {      // End of calculations
      if( _Ntry == 0 ) {      // No retry mode
        MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
        MPI_Send(end_tb_flag,_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);

        for( int i = 0 ; i < min(nproc-2,tbsize-1) ; i++ ) {
          MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
          MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);

          int nameno = atoi(_Workerlog[wid].name.back().c_str());
          _Namelog[nameno-1].rcode[0][0] = 1;
          _Namelog[nameno-1].rcode[1][0] = ir[0];
          _Namelog[nameno-1].rcode[2][0] = ir[1];

          _Workerlog[wid].rcode.back() = 1;

          MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
          MPI_Send(end_tb_flag,_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);
        }

        if( tbsize < nproc-1 ) {
          for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
          delete [] ctable;
          delete [] param;
          return 1;
        }
        else {
          for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
          delete [] ctable;
          delete [] param;
          return 0;
        }
      }
      else {          // Retry mode
        for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
        delete [] ctable;
        delete [] param;
        return 1;
      }
    }

    do {
      MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
      MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);
      int nameno = atoi(_Workerlog[wid].name.back().c_str());

      bool wid_check = false;      // Worker ID check flag

      for( int i = _Namelog[nameno-1].worker.size()-1 ; i >= 0 ; i-- ) {
        if( _Namelog[nameno-1].worker[i] == wid ) {
          _Namelog[nameno-1].rcode[0][i] = 1;
          _Namelog[nameno-1].rcode[1][i] = ir[0];
          _Namelog[nameno-1].rcode[2][i] = ir[1];

          if( ir[0] == 0 && (ir[1] == 1 || _Out_option == 0) ) {
            _Namelog[nameno-1].status = _Ntry+1;
          }
          else {
            _Namelog[nameno-1].status ++;
            _Workerlog[wid].failure ++;    // failure counter
          }

          wid_check = true;
          break;
        }
      }

      _Workerlog[wid].rcode.back() = 1;

      if( !wid_check ) {
        cerr << "[ERROR] Woker ID was a mismatch!!" << endl;
        for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
        delete [] ctable;
        delete [] param;
        return 1;
      }

      if( _Workerlog[wid].failure >= _Worker_life ) {  // check worker life
        cerr << "[Warning] Worker " << wid << " starts sleeping." << endl;

        if( --remain_workers == 0 ) {
          cerr << "[ERROR] All wokers were stoped!!" << endl;
          for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
          delete [] ctable;
          delete [] param;
          return 1;
        }

      }
    } while( _Workerlog[wid].failure >= _Worker_life );  // check worker life

    itry = ic;

    for( int j = jstart ; j < tbsize ; j++ ) {
      if( _Namelog[j].status > _Ntry ) {
        jstart = j + 1;
      }
      else {
        break;
      }
    }
  }
}

//============================================================================//
int Mpidp::master1(const int &nproc)
// master process for NO retry
//============================================================================//
{
  int wid;        // Worker id
  int retry = 0;      // Retry counter
  int ir[2];        // 0:RET and 1:FILE flags 
  int tbsize = _Table_list.size();
  char end_tb_flag[] = "EndOfTable";  // Table list END flag
  char *param = new char[_Psize];  // for PARAM data

  // for Table list
  char **ctable = new char*[tbsize];
  for( int i = 0 ; i < tbsize ; i++ ) {
    ctable[i] = new char[_Csize];
  }

  strcpy(param,_Param.c_str());

  // The calculation condition is sent to workers. 
  for( int i = 1 ; i < nproc ; i++ ) {
    MPI_Send(&_Psize,1,MPI_INT,i,100,MPI_COMM_WORLD);
    MPI_Send(param,_Psize,MPI_CHAR,i,200,MPI_COMM_WORLD);
    MPI_Send(&_Csize,1,MPI_INT,i,300,MPI_COMM_WORLD);
    MPI_Send(&_Ndata,1,MPI_INT,i,400,MPI_COMM_WORLD);
    MPI_Send(&_Out_option,1,MPI_INT,i,420,MPI_COMM_WORLD);
  }

  _Namelog = new NameLog[tbsize];
  _Workerlog = new WorkerLog[nproc];

  int nsend = 0;
  int nrecv = 0;
  while(1) {

    int jobid = getNextReadyJobID();

    if( jobid != -1 ) {
      sprintf(_Name,"%05d\t\0",jobid+1);  // Event number
      strcpy(ctable[jobid],_Name);
      strcat(ctable[jobid],_Table_list[jobid].c_str());
      _Namelog[jobid].name = _Name;
      _Namelog[jobid].exec = 1;    // EXEC flag = 1 (fixed)

      _Namelog[jobid].rcode[0].push_back(0);
      _Namelog[jobid].rcode[1].push_back(-1);
      _Namelog[jobid].rcode[2].push_back(0);

      if( nsend < nproc-1 ) {
        _Namelog[jobid].worker.push_back(nsend+1);
        _Workerlog[nsend+1].name.push_back(_Name);
        _Workerlog[nsend+1].rcode.push_back(0);
        _Workerlog[nsend+1].run = 1;
        MPI_Send(&retry,1,MPI_INT,nsend+1,450,MPI_COMM_WORLD);
        MPI_Send(ctable[jobid],_Csize,MPI_CHAR,nsend+1,500,MPI_COMM_WORLD);

        _JobControl[jobid].ready = 0;
        _JobControl[jobid].run = 1;
        nsend++;
      }
      else {

        wid = getNotRunRank(nproc);

        if( wid != -1 ) {
          _Namelog[jobid].worker.push_back(wid);
          _Workerlog[wid].name.push_back(_Name);
          _Workerlog[wid].rcode.push_back(0);
          _Workerlog[wid].run = 1;
          MPI_Send(&retry,1,MPI_INT,wid,450,MPI_COMM_WORLD);
          MPI_Send(ctable[jobid],_Csize,MPI_CHAR,wid,500,MPI_COMM_WORLD);

          _JobControl[jobid].ready = 0;
          _JobControl[jobid].run = 1;
          nsend++;
        }
        else {
          MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
          MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);
          nrecv++;
          int nameno = atoi(_Workerlog[wid].name.back().c_str());
          int jobid0 = nameno - 1;
          _Namelog[jobid0].rcode[0][0] = 1;
          _Namelog[jobid0].rcode[1][0] = ir[0];
          _Namelog[jobid0].rcode[2][0] = ir[1];

          _Workerlog[wid].rcode.back() = 1;
          _Workerlog[wid].run = 0;

          _JobControl[jobid0].ready = 0;
          _JobControl[jobid0].run = 0;
          _JobControl[jobid0].done = 1;
          resetReadyJobID(jobid0);
#ifdef DEBUG_LOG
          writeJobControl();
#endif
        }
      }
    }
    else {
      MPI_Recv(&wid,1,MPI_INT,MPI_ANY_SOURCE,600,MPI_COMM_WORLD,&_Status);
      MPI_Recv(ir,2,MPI_INT,wid,550,MPI_COMM_WORLD,&_Status);
      nrecv++;
      int nameno = atoi(_Workerlog[wid].name.back().c_str());
      int jobid0 = nameno - 1;
      _Namelog[jobid0].rcode[0][0] = 1;
      _Namelog[jobid0].rcode[1][0] = ir[0];
      _Namelog[jobid0].rcode[2][0] = ir[1];

      _Workerlog[wid].rcode.back() = 1;
      _Workerlog[wid].run = 0;

      _JobControl[jobid0].ready = 0;
      _JobControl[jobid0].run = 0;
      _JobControl[jobid0].done = 1;
      resetReadyJobID(jobid0);
#ifdef DEBUG_LOG
      writeJobControl();
#endif

      if( nrecv == tbsize ) break;
    }
  }

  // finalization 
  for( int i = 0 ; i < nproc-1 ; i++ ) {
    MPI_Send(&retry,1,MPI_INT,i+1,450,MPI_COMM_WORLD);
    MPI_Send(end_tb_flag,_Csize,MPI_CHAR,i+1,500,MPI_COMM_WORLD);
  }

  for( int i = 0 ; i < tbsize ; i++ ) delete [] ctable[i];
  delete [] ctable;
  delete [] param;

  if( tbsize < nproc-1 ) {
    return 1;
  }
  else {
    return 0;
  }
}

#ifndef SYSTEMCALL
//============================================================================//
void Mpidp::worker(int &myid,char *hostname,int argc,char *argv[])
// worker process for function call
//============================================================================//
{
  int    argc2;    // # of mpidp comand line parameters
  int    wargc;    // # of application command line parameters
  int    retry;
  int    ir[2];
  struct stat  buf;

  MPI_Recv(&_Psize,1,MPI_INT,0,100,MPI_COMM_WORLD,&_Status);
  char *param = new char[_Psize];
  MPI_Recv(param,_Psize,MPI_CHAR,0,200,MPI_COMM_WORLD,&_Status);
  _Param = param;

  if( !strncmp(param,"MPIDP",5) ) {
    cerr << "[ERROR] [PARAM=] was not found in table file!!" << endl;
    exit(1);
  }

  MPI_Recv(&_Csize,1,MPI_INT,0,300,MPI_COMM_WORLD,&_Status);
  MPI_Recv(&_Ndata,1,MPI_INT,0,400,MPI_COMM_WORLD,&_Status);
  MPI_Recv(&_Out_option,1,MPI_INT,0,420,MPI_COMM_WORLD,&_Status);

  char *ctable = new char[_Csize];

  // Correction of a bug
  int arglen = max(_Csize,_Psize);
  for( int i = 0 ; i < argc ; i++ ) {
    if( strlen(argv[i]) >= arglen ) {
      arglen = strlen(argv[i]) + 1;
    }
  }
  // Bug fix
  int nwargv = argc + _Ndata*2;
  for( int i = 1 ; i < _Psize-2 ; i++ ) {
    if( param[i] == ' ' ) nwargv++;
  }
  char **wargv = new char*[nwargv];
  for( int i = 0 ; i < nwargv ; i++ ) {
    wargv[i] = new char[arglen];
  }

  argc2 = argument(argc,argv,wargv);

  while(1) {
    MPI_Recv(&retry,1,MPI_INT,0,450,MPI_COMM_WORLD,&_Status);
    MPI_Recv(ctable,_Csize,MPI_CHAR,0,500,MPI_COMM_WORLD,&_Status);
    if( !strncmp(ctable,"EndOfTable",10) ) break;  // Table End flag

    // Preparation using function call
    wargc = for_worker(retry,ctable,argc2,wargv);
    //    cout << _Name << " : " << hostname << "(" << myid << ")" << endl;

    /* for test
    if( _Out_option ) {
      cout << "(int)MPI_Wtime() = " << (int)MPI_Wtime() << endl;
      if( int(MPI_Wtime())%20 == 0 ) {
        cout << "Worker " << myid << " starts sleeping.\n";
        sleep(100000);
      }
    }
    */

    try {
      throw application(wargc,wargv);  // application's main function
    }
    catch(int e) {
      ir[0] = e;
    }
    catch(char *e) {
      cerr << "[ERROR] [application] exception : " << e << endl;
      ir[0] = -1;
    }

    if( _Out_option ) {        // check the output file
      ir[1] = stat(_Out_file.c_str(),&buf);
      if( ir[1] == 0 ) {
        ir[1] = 1;
      }
    }
    else {
      ir[1] = 0;
    }

    MPI_Send(&myid,1,MPI_INT,0,600,MPI_COMM_WORLD);  // Send id
    MPI_Send(ir,2,MPI_INT,0,550,MPI_COMM_WORLD);
  }

  for( int i = 0 ; i < nwargv ; i++ ) delete [] wargv[i];
  delete [] wargv;
  delete [] ctable;
  delete [] param;

  return;
}

#else
//============================================================================//
void Mpidp::worker(int &myid,char *hostname,int argc,char *argv[])
// worker process for system call
//============================================================================//
{
  string  main_argv;    // mpidp command line
  string  argv_joblist;    // system call command line
  int    retry;
  int    ir[2];
  struct stat   buf;

  MPI_Recv(&_Psize,1,MPI_INT,0,100,MPI_COMM_WORLD,&_Status);
  char *param = new char[_Psize];
  MPI_Recv(param,_Psize,MPI_CHAR,0,200,MPI_COMM_WORLD,&_Status);
  _Param = param;

  MPI_Recv(&_Csize,1,MPI_INT,0,300,MPI_COMM_WORLD,&_Status);
  MPI_Recv(&_Ndata,1,MPI_INT,0,400,MPI_COMM_WORLD,&_Status);
  MPI_Recv(&_Out_option,1,MPI_INT,0,420,MPI_COMM_WORLD,&_Status);

  int ia = argument(argc,argv,main_argv);

  if( ia == 0 && !strncmp(param,"MPIDP",5) ) {
    cerr << "[ERROR] [PARAM=] was not found in table file!!" << endl;
    exit(1);
  }
  else if( ia == 1 && strncmp(param,"MPIDP",5) ) {
    if( myid == 1 ) {
      cerr << "[WARNING] [PARAM=] in table file was ignored." << endl;
    }
  }

  char *ctable = new char[_Csize];

  while(1) {
    MPI_Recv(&retry,1,MPI_INT,0,450,MPI_COMM_WORLD,&_Status);
    MPI_Recv(ctable,_Csize,MPI_CHAR,0,500,MPI_COMM_WORLD,&_Status);
    if( !strncmp(ctable,"EndOfTable",10) ) break;  // Table End flag

    argv_joblist = main_argv;
    if( ia == 0 ) {
      argv_joblist += ' ';
      argv_joblist += _Param;
    }

    // Preparation using system call
    for_worker(retry,ctable,ia,argv_joblist);
    //    cout << _Name << " : " << hostname << "(" << myid << ")" << endl;

    /* for test
    if( _Out_option ) {
      cout << "(int)MPI_Wtime() = " << (int)MPI_Wtime() << endl;
      if( int(MPI_Wtime())%20 == 0 ) {
        cout << "Worker " << myid << " starts sleeping.\n";
        sleep(100000);
      }
    }
    */

    ir[0] = system(argv_joblist.c_str());  // system call for application

    if( _Out_option ) {        // check the output file
      ir[1] = stat(_Out_file.c_str(),&buf);
      if( ir[1] == 0 ) {
        ir[1] = 1;
      }
    }
    else {
      ir[1] = 0;
    }

    MPI_Send(&myid,1,MPI_INT,0,600,MPI_COMM_WORLD);  // Send id
    MPI_Send(ir,2,MPI_INT,0,550,MPI_COMM_WORLD);
  }

  delete [] ctable;
  delete [] param;

  return;
}
#endif

//============================================================================//
int Mpidp::argument(int argc,char *argv[],char **wargv)
// Procedure of options for function call version
//============================================================================//
{
  int ic = 0;

  for( int i = 0 ; i < argc ; i++ ) {
    if( !strncmp(argv[i],"-tb",3) ||
        !strncmp(argv[i],"-ot",3) ||
        !strncmp(argv[i],"-rt",3) ||
        !strncmp(argv[i],"-wl",3) ||
        !strncmp(argv[i],"-jo",3) ||
        !strncmp(argv[i],"-lg",3) ) {
      i++;
    }
    else {
      strcpy(wargv[ic++],argv[i]);
    }
  }

  return ic;
}

//============================================================================//
int Mpidp::argument(int argc,char *argv[],string &main_argv)
// Procedure of options for system call version
//============================================================================//
{
  int iflag = 1;

  for( int i = 1 ; i < argc ; i++ ) {
    if( !strncmp(argv[i],"-pg",3) ) {
      main_argv = argv[++i];
      iflag = 0;
    }
    else if( !strncmp(argv[i],"-tb",3) ||
             !strncmp(argv[i],"-ot",3) ||
             !strncmp(argv[i],"-rt",3) ||
             !strncmp(argv[i],"-wl",3) ||
             !strncmp(argv[i],"-jo",3) ||
             !strncmp(argv[i],"-lg",3) ) {
      i++;
    }
    else {
      main_argv += ' ';
      main_argv += argv[i];
    }
  }

  return iflag;
}

//============================================================================//
int Mpidp::for_worker(int &retry,char *ctable,int argc2,char **wargv)
// Preparation using function call version by workers
//============================================================================//
{
  string  position;
  string  tstock;
  string  sparam = _Param;
  char    tag[5], *elem;
  char    *saveptr;

  strcpy(_Name,strtok_r(ctable,"\t",&saveptr));

  for( int i = 0 ; i < _Ndata-1 ; i++ ) {
    sprintf(tag,"$%d",i+1);
    position = tag;
    tstock = strtok_r(NULL,"\t",&saveptr);

    if( _Out_option == i+1 ) {
      if( retry ) {
        sprintf(tag,"%d",retry);
        tstock += ".";
        tstock += tag;
      }
      _Out_file = tstock;
    }

    // character is replaced
    sparam = replace_pattern(sparam,position,tstock);
  }

  char *param = new char[sparam.size()+1];
  strcpy(param,sparam.c_str());
  strcpy(wargv[argc2++],strtok_r(param," ",&saveptr));

  while( (elem = strtok_r(NULL," ",&saveptr)) ) {
    strcpy(wargv[argc2++],elem);
  }

  /*
  for( int i = 0 ; i < argc2 ; i++ ) {
    cout << wargv[i] << " ";
  }
  cout << endl;
  */

  delete [] param;

  return argc2;
}

//============================================================================//
void Mpidp::for_worker(int &retry,char *ctable,int &ia,
           string &argv_joblist)
// Preparation using system call version by workers
//============================================================================//
{
  string  position;
  string  tstock;
  char    tag[5];
  char    *saveptr;

  strcpy(_Name,strtok_r(ctable,"\t",&saveptr));

  if( ia ) {
    argv_joblist = strtok_r(NULL,"\t",&saveptr) + argv_joblist;
  }
  else {
    for( int i = 0 ; i < _Ndata-1 ; i++ ) {
      sprintf(tag,"$%d",i+1);
      position = tag;
      tstock = strtok_r(NULL,"\t",&saveptr);

      if( _Out_option == i+1 ) {
        if( retry ) {
          sprintf(tag,"%d",retry);
          tstock += ".";
          tstock += tag;
        }
        _Out_file = tstock;
      }

      // character is replaced
      argv_joblist = replace_pattern(argv_joblist,position,tstock);
    }
  }

  //  cout << argv_joblist << endl;

  return;
}

//============================================================================//
string Mpidp::replace_pattern(const string &pattern, const string &position, const string &option) {
// the pattern of a character is replaced 
//============================================================================//
  string  command;
  int  pos_before = 0;
  int  pos = 0;
  int  len = position.size();
  
  while( (pos = pattern.find(position, pos)) != std::string::npos) {
    command.append(pattern, pos_before, pos - pos_before);
    command.append(option);
    pos += len;
    pos_before = pos;
  }

  command.append(pattern, pos_before, pattern.size() - pos_before);

  return command;
}

//============================================================================//
void Mpidp::write_table(const int &nproc,ofstream &logout)
// write JOB and Workers report
//============================================================================//
{
  // JOB table
  logout << "JOB table :" << endl;

  for( int i = 0 ; i < _Table_list.size() ; i++ ) {
    logout << _Namelog[i].name << " EXEC=" << _Namelog[i].exec;

    for( int j = 0 ; j < _Namelog[i].worker.size() ; j++ ) {
      logout << " (WID=" << _Namelog[i].worker[j];
      logout << " END=" << _Namelog[i].rcode[0][j];
      logout << " RET=" << _Namelog[i].rcode[1][j];
      logout << " FILE=" << _Namelog[i].rcode[2][j] << ")";
    }
    logout << endl;
  }

  // Worker table
  logout << "\nWorker table :" << endl;

  for( int i = 1 ; i < nproc ; i++ ) {
    logout << i << "\t" << _Workerlog[i].name.size();

    for( int j = 0 ; j < _Workerlog[i].name.size() ; j++ ) {
      logout << "\t" << _Workerlog[i].name[j] << "\t" << _Workerlog[i].rcode[j];
    }
    logout << endl;
  }

  delete [] _Workerlog;
  delete [] _Namelog;

  return;
}

//============================================================================//
int Mpidp::getNextReadyJobID()
//  Get next ready Job id
//============================================================================//
{
  int jobid = -1;

  for( int i = 0 ; i < _Table_list.size() ; i++ ) {
    if( _JobControl[i].ready == 1 &&  _JobControl[i].done == 0 && _JobControl[i].run == 0 ) {
      jobid = i;
    }
  }
  return jobid;
}

//============================================================================//
void Mpidp::resetReadyJobID(int &jobid)
// reset _JobControl[].ready flag
//============================================================================//
{
  for( int i = 0 ; i < _JobControl[jobid].depended.size() ; i++ ) {
    int task_id = _JobControl[jobid].depended[i];
    int job_id = _jobid_map[task_id];
    if( checkDependTaskID(job_id) ) {
      _JobControl[job_id].ready = 1;
    }
  }
}

//============================================================================//
int Mpidp::checkDependTaskID(int &jobid)
// check _JobControl[].depend ids whether done(1) all or not(0).
//============================================================================//
{
  int ret = 1;

  for( int i = 0 ; i < _JobControl[jobid].depend.size() ; i++ ) {
    int task_id = _JobControl[jobid].depend[i];
    int job_id = _jobid_map[task_id];
    int done = _JobControl[job_id].done;
    ret = ret * done;
    if(done == 0) break;
  }
  return ret;
}

//============================================================================//
void Mpidp::writeJobControl()
// write _JobControl[]
//============================================================================//
{
  cout << "jobid taskid ready run done depend depended" << endl;
  for( int i = 0 ; i < _Table_list.size() ; i++ ) {
    cout << i << " " << _JobControl[i].task_id << " " << _JobControl[i].ready;
    cout << " " << _JobControl[i].run << " " << _JobControl[i].done << " <";
    for( int j = 0 ; j < _JobControl[i].depend.size() ; j++ ) {
      cout << _JobControl[i].depend[j] << ",";
    }
    cout << "> <";
    for( int j = 0 ; j < _JobControl[i].depended.size() ; j++ ) {
      cout << _JobControl[i].depended[j] << ",";
    }
    cout << ">" << endl;
  }
}

//============================================================================//
int Mpidp::count_comma(const string &s0)
// Count comma
//============================================================================//
{
  int n = 0;
  string s1;

  s1 = s0;
  while(1) {
    size_t i = s1.rfind(',', s1.length());
    if (i != string::npos) {
      s1 = s1.substr(0, i);
      n++;
    }
    else {
      break;
    }
  }
  return n;
}

//============================================================================//
void Mpidp::clear_JobControl()
// clear _JobControl
//============================================================================//
{
  for( int i = 0 ; i < _Table_list.size() ; i++ ) {
    _JobControl[i].task_id   = 0;
    _JobControl[i].ready     = 0;
    _JobControl[i].run       = 0;
    _JobControl[i].done      = 0;
  }
}

//============================================================================//
int Mpidp::getNotRunRank(const int &nproc)
// get rank at run = 0 
//============================================================================//
{
  for( int i = 1 ; i < nproc ; i++ ) {
    if(_Workerlog[i].run == 0 ) return i;
  }
  return -1;
}
