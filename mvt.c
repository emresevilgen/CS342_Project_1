#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <pthread.h>
#include <sys/file.h>
#include <sys/wait.h> 
#include <sys/time.h>


int findNumberOfLinesInFile(char* filePath);
void splitFile(int s, int k, int l);
void readVectorFile();
void mapper(char* splitFilePath, char* mapperFilePath);
void* mapperRunner (void* k);
void reduce(char* resultfile, int k);
void* reduceRunner (void *n);
void deleteIntermediateFiles();

int size;
int * vector ;
int * result;
char * vectorfile;
char * resultfile;
char * matrixfile;
struct timeval start_time, end_time;

int main(int argc, char *argv[]) {
  int start;
  start = gettimeofday(&start_time, NULL);

  matrixfile = argv[1];
  vectorfile = argv[2];
  resultfile = argv[3];
  int k = atoi(argv[4]);

  size = findNumberOfLinesInFile(vectorfile);
  vector = malloc(size * sizeof(int)+1);
  result = malloc(size * sizeof(int)+1);

  readVectorFile();
  int l = findNumberOfLinesInFile(matrixfile);
  int s = l/k;
  splitFile(s, k, l);

  pthread_t tid; 
  pthread_attr_t attr; 
  for(int i = 1; i <= k; i++){
      char temp [15];
      sprintf(temp, "%d", i);
      pthread_attr_init( &attr);
      pthread_create(&tid, &attr, mapperRunner, temp);
      pthread_join(tid, NULL);
  }
  for(int i = 0; i <size; i++)
    result[i] = 0;
  int end = gettimeofday(&end_time, NULL);
  
  printf("The program executed in %ld seconds, %ld microseconds.\n",
      (end_time.tv_sec - start_time.tv_sec), 
      (end_time.tv_usec - start_time.tv_usec));
  
  char count[15];
  sprintf(count, "%d", k);
  pthread_attr_init( &attr);
  pthread_create(&tid, &attr, reduceRunner, count);
  pthread_join(tid, NULL);
  
  deleteIntermediateFiles();

  exit(0);
}

int findNumberOfLinesInFile(char* filePath) {
  int fp = open(filePath, O_RDONLY);

  if(fp < 0){
    printf("Could not open the %s %s", filePath, "\n");
    return -1;
  }
  int l = 0;
    char buffer[1024];
    int length = read(fp, &buffer, 1024);
    buffer[length] = '\0';
    char * token = strtok(buffer, " \t\n");
    
    while( token != NULL)
    {
        token = strtok(NULL, " \t\n");
        token = strtok(NULL, " \t\n");  
        l++;
    }

  close(fp); 
  return l;
  
}

void splitFile(int s, int k, int l) {
  FILE* fp_in = fopen (matrixfile, "r");

  if(fp_in == NULL) {
    printf("Could not open the %s %s", matrixfile, "\n");
    return;
  }

  FILE* fp_out;
  
  char * line = NULL;
  size_t len = 0;
  ssize_t read;  
  
  char outputFilename[15];
  int filecounter = 0; 
  int linecounter = 0;
  int rem = l % k; 

  while (linecounter < l) {
    read = getline(&line, &len, fp_in);
    if (read == -1) {
      break;
    }
    if (linecounter % s == 0) {
      if (linecounter + rem < l) {
        if (linecounter != 0)
          fclose(fp_out);
        filecounter++;
        sprintf(outputFilename, "split%d", filecounter);
        fp_out = fopen(outputFilename, "w");
        if (fp_out == NULL){
          printf("Could not write to the %s %s", outputFilename, "\n");
          return;
        }
      }
    }
    fwrite(line, read, 1, fp_out);
    linecounter++;
  }     
 
  fclose(fp_in);
  fclose(fp_out);
}

void readVectorFile() {
  FILE* fp_in = fopen (vectorfile, "r");

  if(fp_in == NULL) {
    printf("Could not open the %s %s", vectorfile, "\n");
    return;
  }
  
  char * line = NULL;
  size_t len = 0;
  ssize_t read;  
  for(int i = 0; i < size; i++) {
    read = getline(&line, &len, fp_in);
    if (read == -1)
      break;
    
    char delim[] = " ";
    char* ptr = strtok(line, delim);
    int index = atoi(ptr);
    int value = atoi(strtok(NULL, delim));
    vector[index - 1] = value;
  }

}

void mapper(char* splitFilePath, char* mapperFilePath) {
  FILE* fp_in = fopen(splitFilePath, "r");
  
  if(fp_in == NULL) {
    printf("Could not open the %s %s", splitFilePath, "\n");
    return;
  }

  if(fp_in == NULL) {
    printf("Could not open the %s %s", mapperFilePath, "\n");
    return;
  }

  char * line = NULL;
  size_t len = 0;
  ssize_t read;  
  int partialResult[size];
  for(int i = 0; i <size; i++)
    partialResult[i] = 0;

  while ((read = getline(&line, &len, fp_in)) != -1) {
    char delim[] = " ";
    int row = atoi(strtok(line, delim));
    int col = atoi(strtok(NULL, delim));
    int value = atoi(strtok(NULL, delim));
    partialResult[row -1] = partialResult[row - 1] + (value * vector[col - 1]);
   
  }

  fclose(fp_in);

    FILE *fp_out = fopen(mapperFilePath, "w+");
    if(fp_out == NULL)
        printf("could not open file.");

  for(int i = 0; i < size; i++) {
    int val = partialResult[i];
    if (val != 0) {
      fprintf(fp_out, "%d %d\n", i+1 , val);
    }
  }
  fclose(fp_out);
}

void* mapperRunner (void* k) {
    int n = atoi(k);
    char splitFilepath[15];
    snprintf(splitFilepath, 15, "split%d",  n);
            
    char mapperFilepath[12];
    snprintf(mapperFilepath, 12, "mapper%d",  n);
    mapper(splitFilepath, mapperFilepath);
    pthread_exit(0);
}

void reduce(char* resultfile, int k){
  for(int i = 0; i < size; i++)
    result[i] = 0;
  for(int i = 1; i <=k; i++) {
    char mapperBuf[12];
    snprintf(mapperBuf, 12, "mapper%d",  i);

    int fd = open (mapperBuf, O_RDONLY);

    char buffer[4096];
    int length = read(fd, &buffer, 4096);
    buffer[length] = '\0';
    char * token = strtok(buffer, " \t\n");

    int value;
    int row;
    while( token != NULL) {
        row = atoi(token);
        token = strtok(NULL, " \t\n");
        value = atoi(token);
        token = strtok(NULL, " \t\n");
        result[row -1] = result[row - 1] + value;
    }
    close(fd);
    }
  FILE *fptr = fopen(resultfile, "w+");
  if(fptr == NULL)
    printf("could not open file.");
  for(int i = 0; i < size; i++) {
    fprintf(fptr, "%d %d\n", i+1 , result[i]);
  }
  fclose(fptr);
  exit(-1);
}

void* reduceRunner (void* n) {
    int k = atoi(n);
    reduce(resultfile, k);
    pthread_exit(0);

}

void deleteIntermediateFiles(){
  int filecounter = 1;
  char outputFilename[15];
  sprintf(outputFilename, "split%d", filecounter);
  while (remove(outputFilename) == 0){
    ++filecounter;
    sprintf(outputFilename, "split%d", filecounter);
  }

  filecounter = 1;
  sprintf(outputFilename, "mapper%d", filecounter);
  while (remove(outputFilename) == 0){
    ++filecounter;
    sprintf(outputFilename, "mapper%d", filecounter);
  }
}