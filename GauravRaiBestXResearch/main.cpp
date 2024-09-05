#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <queue>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <dirent.h>

using namespace std;


struct FeedData
{
    public :
        FeedData(const string symbol, const string feedData)
        {
            size_t dotPosition = symbol.find_last_of('.');
            if(dotPosition == std::string::npos ||
            dotPosition == 0)
            {
                instSymbol = symbol;
            }
            else
            {
                instSymbol = symbol.substr(0,dotPosition);
            }

            std::istringstream stream(feedData);
            getline(stream,timeStamp,',');
            getline(stream,price,',');
            getline(stream,size,',');
            getline(stream,exchange,',');
            getline(stream,type);
        
        }

        string  getStr () const
        {
            return instSymbol+", "+timeStamp+","+price+","+size+","+exchange+","+type+"\n";
        }

        bool operator ()(const shared_ptr<FeedData>& f1, const shared_ptr<FeedData>& f2)
        {
            if(f1->timeStamp == f2->timeStamp)
            {
                return f1->instSymbol<=f2->instSymbol;
            }
            return f1->timeStamp<f2->timeStamp;
        }

        friend ostream& operator << (ostream& os, const FeedData& p);

        string instSymbol;
        string timeStamp;
        string price;
        string size;
        string exchange;
        string type;
        
};

// Comparator for the FeedData for priorityQueue
class Compare
{
    public:
        bool operator ()(const shared_ptr<FeedData>& below, const shared_ptr<FeedData>& above)
        {
            if(below->timeStamp == above->timeStamp)
            {
                return below->instSymbol>above->instSymbol;
            }
            return below->timeStamp > above->timeStamp;
        }
};

std::ostream & operator<<(std::ostream &os, const FeedData& p)
{
    return os <<p.timeStamp<<", "<<" "<<p.instSymbol<<", "<<p.price<<", "<<p.size<<", "<<p.type<<"\n";
}

class FileMerger
{
    public:

        void init()
        {
            this->minUnreadTimeStamp = "5000-03-05 00:00:00.000";
            this->read = true;
            this->terminate = false;
            this->fileIndexMap.clear();
            this->fetchFile(this->inputDir);
        }
        
        // Fetching the files from the input folder and putting in fileIndexMap
        void fetchFile(const string& inputDir)
        {
            if(auto dir = opendir(inputDir.c_str()))
            {
                while( auto file = readdir(dir))
                {
                    if( !file->d_name || file->d_name[0] == '.')
                    {
                        continue;
                    }
                    else
                    {
                        //std::cout<<"Coming here file name is "<<file->d_name<<" \n";
                        fileIndexMap[file->d_name] = 1;
                    }
                }
            }
        }

        void persistTheDataToMainFile()
        {
            while(!this->terminate)
            {
                unique_lock<mutex> lock(this->pqMtx);
                cv.wait(lock,[this](){return !this->read;});
                ofstream outputFile;
                outputFile.open (this->outputFile,std::ios_base::app);

                // pop out the elements from the priority Queue untill top element timestamp is less than last minunreadtimeStamp 
                while(!pq.empty() && (pq.top()->timeStamp.compare(this->minUnreadTimeStamp)<0))
                {
                    cout<<"Popping the data \n";
                    cout<<pq.top()->getStr()<<" \n";
                    outputFile<<pq.top()->getStr();
                    pq.pop();
                }

                outputFile.close();

                // After writing we can again start reading from the files 
                this->read = true;
                this->cv.notify_all();
            }
            
            // if all the files are read we can write the remaining entries in priority Queue to the outputfile
            if(terminate && !pq.empty())
            {
                ofstream outputFile;
                outputFile.open (this->outputFile,std::ios_base::app);
                while(!pq.empty())
                {
                    cout<<"Popping the data \n";
                    cout<<pq.top()->getStr()<<" \n";
                    outputFile<<pq.top()->getStr();
                    pq.pop();
                }
                outputFile.close();
            }
        }

        void process ()
        {
            this->init();
            
            // Writing the header of output file 
            ofstream file(this->outputFile, std::ofstream::trunc);
            file<<"Symbol, Timestamp, Price, Size, Exchange, Type\n";
            file.close(); 

            if(this->fileIndexMap.size() == 0)
            {
                cout<<"No valid Files in input folder \n";
                return;
            }

            // Creating a thread which will persist the data to output file 
            thread t(&FileMerger::persistTheDataToMainFile, this);
    
            int i = 0;

            // Reading until there is file left to be read 
            while(this->fileIndexMap.size()>0)
            {
                unique_lock<mutex> lock(this->pqMtx);
                this->cv.wait(lock, [this](){return this->read;});

                // updating minUnreadTimeStamp to maxdate to updated the minimum unread line timestamp
                this->minUnreadTimeStamp = "2028-03-05 10:00:00.123";

                for(auto& itr : this->fileIndexMap)
                {
                    ifstream file(this->inputDir+(itr.first));
                    // if the file is corrupted then logging and ignoring this file 
                    if(!file.is_open())
                    {
                        cout<<"Can't open the file "<<itr.first<<" \n";
                        this->fileIndexMap.erase(itr.first);
                    }
                    else
                    {
                        int index  = 0;
                        string readLine;

                        // Skipping the lines which we have already read
                        while(index<itr.second && getline(file,readLine))
                        {
                            cout<<"Skipping the reading line "<<readLine<<" \n";
                            index++;
                        }

                        // updating the last read line
                        itr.second = itr.second+1;

                        // reading the next line from this file else removing it from the fileIndex Map                        
                        if(getline(file,readLine))
                        {
                            cout<<"Pushing the line "<<readLine<<" \n";

                            // Reading the first unread line fromt this file.
                            this->pq.push(make_shared<FeedData>(itr.first,readLine));
                            
                            // if its the last file read all the remaining entries till last line
                            if(this->fileIndexMap.size()==1)
                            {
                                while(getline(file,readLine))
                                {
                                    cout<<"Pushing the line "<<readLine<<" \n";
                                    this->pq.push(make_shared<FeedData>(itr.first,readLine)); 
                                }

                                // Removing the file from fileIndexMap after reading all the entries
                                this->fileIndexMap.erase(itr.first);
                            }

                            // if its not the last line of this file updating last unread time stamp
                            if(getline(file,readLine))
                            {
                                auto data = FeedData(itr.first,readLine);
                                cout<<"Lasting unread Line"<<readLine<<" \n";
                                if(data.timeStamp.compare(this->minUnreadTimeStamp)<0)
                                {
                                    this->minUnreadTimeStamp = data.timeStamp;
                                }
                            }
                        }
                        else
                        {
                            this->fileIndexMap.erase(itr.first);
                        }
                    }

                    // closing this file 
                    file.close();
                }



                // Once we have completed this cycle of read we can write to the main file 
                // until the timestamp is greater than minimum last unread entry timestamp(minUnreadTimeStamp)
                this->read = false;

                // if all file are done reading we can terminate
                if(this->fileIndexMap.size()==0)
                {
                    cout<<" Terminating the Reading \n";
                    this->terminate = true;
                }
                this->cv.notify_all();
            }
            // Waiting for the writing thread to complete 
            t.join();
        }

        FileMerger():
        inputDir("input/"),// Input file folder 
        outputFile("MultiplexedFile.txt"), // output file name 
        minUnreadTimeStamp("5000-03-05 00:00:00.000"), // last unread timestamp
        read(true),
        terminate(false)
        {
        }

    private:
        string inputDir;
        priority_queue<shared_ptr<FeedData>, vector<shared_ptr<FeedData> >, Compare> pq;
        mutex pqMtx;
        condition_variable cv;
        string outputFile;
        unordered_map<string, int> fileIndexMap;
        string minUnreadTimeStamp;
        bool read;
        bool terminate;
};

int main()
{
    auto f = FileMerger();
    f.process();
    return 0;
}