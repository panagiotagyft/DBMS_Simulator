#include <stdbool.h>
#ifndef SHT_TABLE_H
#define SHT_TABLE_H
#include <record.h>
#include <ht_table.h>

// Has info about the SHTable
typedef struct {
    int fileDesc;       /* αναγνωριστικός αριθμός ανοίγματος αρχείου από το επίπεδο block */
    BF_Block* InfoAboutHTable;  /*το block στο οποίο είναι αποθηκευμένη*/
    int** HashTable;            /* ο πίνακας κατακερματισμού.[bucketId, lastBucketWithFreeSpace]*/
    long int numOfBuckets;      /* το πλήθος των αρχικών “κάδων” του αρχείου κατακερματισμού */ 
    int recordSize;             /*η χωρητικότητα μίας εγγραφής(Name, BLockId)*/
    int numberOfRecordsInBlock; /*η χωρητικότητα ενός block σε εγγραφές*/
} SHT_info;

typedef struct {
    int numberOfRecords;     /*τον αριθμό των εγγραφών στο συγκεκριμένο block*/
    int nextBlockId;         /*έναν δείκτη στο επόμενο block δεδομένων (block υπερχείλισης)*/
} SHT_block_info;

typedef struct {
    char name[15];
    int blockId;
} Record_Secondary;

/*Η συνάρτηση SHT_CreateSecondaryIndex χρησιμοποιείται για τη δημιουργία
και κατάλληλη αρχικοποίηση ενός αρχείου δευτερεύοντος κατακερματισμού με
όνομα sfileName για το αρχείο πρωτεύοντος κατακερματισμού fileName. Σε
περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται 0, ενώ σε διαφορετική
περίπτωση -1.*/
int SHT_CreateSecondaryIndex(char *sfileName, /* όνομα αρχείου δευτερεύοντος ευρετηρίου*/int buckets, /* αριθμός κάδων κατακερματισμού*/char* fileName /* όνομα αρχείου πρωτεύοντος ευρετηρίου*/);

/* Η συνάρτηση SHT_OpenSecondaryIndex ανοίγει το αρχείο με όνομα sfileName
και διαβάζει από το πρώτο μπλοκ την πληροφορία που αφορά το δευτερεύον
ευρετήριο κατακερματισμού.*/
SHT_info* SHT_OpenSecondaryIndex(char *sfileName /* όνομα αρχείου δευτερεύοντος ευρετηρίου */);

/*Η συνάρτηση SHT_CloseSecondaryIndex κλείνει το αρχείο που προσδιορίζεται
μέσα στη δομή header_info. Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται
0, ενώ σε διαφορετική περίπτωση -1. Η συνάρτηση είναι υπεύθυνη και για την
αποδέσμευση της μνήμης που καταλαμβάνει η δομή που περάστηκε ως παράμετρος,
στην περίπτωση που το κλείσιμο πραγματοποιήθηκε επιτυχώς.*/
int SHT_CloseSecondaryIndex( SHT_info* header_info );

/*Η συνάρτηση SHT_SecondaryInsertEntry χρησιμοποιείται για την εισαγωγή μιας
εγγραφής στο αρχείο κατακερματισμού. Οι πληροφορίες που αφορούν το αρχείο
βρίσκονται στη δομή header_info, ενώ η εγγραφή προς εισαγωγή προσδιορίζεται
από τη δομή record και το block του πρωτεύοντος ευρετηρίου που υπάρχει η εγγραφή
προς εισαγωγή. Σε περίπτωση που εκτελεστεί επιτυχώς, επιστρέφεται 0, ενώ σε
διαφορετική περίπτωση -1.*/
int SHT_SecondaryInsertEntry(SHT_info* header_info, /* επικεφαλίδα του δευτερεύοντος ευρετηρίου*/Record record, /* η εγγραφή για την οποία έχουμε εισαγωγή στο δευτερεύον ευρετήριο*/int block_id /* το μπλοκ του αρχείου κατακερματισμού στο οποίο έγινε η εισαγωγή */);

/*Η συνάρτηση αυτή χρησιμοποιείται για την εκτύπωση όλων των εγγραφών που
υπάρχουν στο αρχείο κατακερματισμού οι οποίες έχουν τιμή στο πεδίο-κλειδί
του δευτερεύοντος ευρετηρίου ίση με name. Η πρώτη δομή περιέχει πληροφορίες
για το αρχείο κατακερματισμού, όπως αυτές είχαν επιστραφεί κατά το άνοιγμά
του. Η δεύτερη δομή περιέχει πληροφορίες για το δευτερεύον ευρετήριο όπως
αυτές είχαν επιστραφεί από την SHT_OpenIndex. Για κάθε εγγραφή που υπάρχει
στο αρχείο και έχει όνομα ίσο με value, εκτυπώνονται τα περιεχόμενά της
(συμπεριλαμβανομένου και του πεδίου-κλειδιού). Να επιστρέφεται επίσης το
πλήθος των blocks που διαβάστηκαν μέχρι να βρεθούν όλες οι εγγραφές. Σε
περίπτωση λάθους επιστρέφει -1.*/
int SHT_SecondaryGetAllEntries(HT_info* ht_info, /* επικεφαλίδα του αρχείου πρωτεύοντος ευρετηρίου*/SHT_info* header_info, /* επικεφαλίδα του αρχείου δευτερεύοντος ευρετηρίου*/char* name /* το όνομα στο οποίο γίνεται αναζήτηση */);

int charHashFunction(char*, int);       // Our hash function for string

bool checkIfExists(int*, int, int);     // Checks if id is in the array.

int HashStatistics_Secondary(char* );   // Statistics function

bool isSecondaryIndex(char* );      // check if the examining string is the same as the secondary index name string

void print_Metadata_SecondaryIndex(SHT_info* );     // print metadata

int print_SecondaryIndex(SHT_info* );       // print secondary index file

#endif // SHT_FILE_H
