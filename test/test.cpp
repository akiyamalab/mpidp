#include <fstream>
#include <iostream>
#include <cstring>
#include <algorithm>
#include <cctype>
#include <string>

using namespace std;

//============================================================================//
int main(int argc,char *argv[])
//============================================================================//
{
  char *PATH_INPUT;
  char *PATH_OUTPUT;

  // for options
  for( int i = 1 ; i < argc ; i++ ) {
    if( !strncmp(argv[i],"-i",2) ) {
      PATH_INPUT = argv[++i];
//      cout << "path of input file option : -i " << PATH_INPUT << endl;
    }
    else if( !strncmp(argv[i],"-o",2) ) {
      PATH_OUTPUT = argv[++i];
//      cout << "path of output file option : -o " << PATH_OUTPUT << endl;
    }
  }

  string filename = PATH_INPUT;
  string filename1 = "";
  string filename2 = "";
  string filename3 = "";
  int no_inputfile = 1;
  size_t i = filename.rfind(',', filename.length());
  if (i != string::npos) {
    filename1 = filename.substr(i+1, filename.length() - i);
    filename2 = filename.substr(0, i);
//cout << "file1=" << filename1 << endl;
//cout << "file2=" << filename2 << endl;
/*
    size_t j = filename.rfind(',', filename.length());
    if (j != string::npos) {
      filename2 = filename.substr(j+1, filename.length() - j);
      filename3 = filename.substr(0, j);
    }
*/
  }
//cout << "file3=" << filename3 << endl;

/*
  size_t i = filename.rfind('.', filename.length());
  if (i != string::npos) {
    ext1 = filename.substr(i+1, filename.length() - i);
  }
cout << "ext1=" << ext1 << endl;
*/

  std::ifstream in_;  // input file stream
  std::ifstream in_1; // input file1 stream
  std::ifstream in_2; // input file2 stream
  if (filename2 != "") {
    in_2.open(filename2.c_str());
    if (!in_2) {
      cout << "can't open input file2:" << filename2 << endl;
      exit(1);
    }

    in_1.open(filename1.c_str());
    if (!in_1) {
      cout << "can't open input file1:" << filename1 << endl;
      exit(1);
    }
    no_inputfile = 2;
  }
  else {
    //in_.open(PATH_INPUT);
    in_.open(filename.c_str());
//cout << "filename=" << filename << endl;
    if (!in_) {
      cout << "can't open input file:" << PATH_INPUT << endl;
      exit(1);
    }
  }

  std::ofstream out;
  out.open(PATH_OUTPUT);
  if (!out) {
    cout << "can't open output file:" << PATH_OUTPUT << endl;
    exit(1);
  }

  if (no_inputfile == 2) {
    std::string line1 = "";
    std::string contents = "";
    while (!in_1.eof()) {
      std::getline(in_1, line1);
      if (line1.length() != 0) {
        contents = contents + line1;
      }
    }
    std::string line2 = "";
    while (!in_2.eof()) {
      std::getline(in_2, line2);
      if (line2.length() != 0) {
        contents = contents + line2;
      }
    }
    out << contents << endl;
  }
  else {
    std::string line = "";
    std::string contents = "";
    while (!in_.eof()) {
      std::getline(in_, line);
      if (line.length() != 0) {
        contents = contents + line;
//cout << "line xx=" << line << endl;
      }
    }
//cout << "contents=" << contents << endl;
    out << contents << endl;
  }

  in_.close();
  out.close();

  return 0;
}
