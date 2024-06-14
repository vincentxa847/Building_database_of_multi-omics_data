# Using multi-omics data to build a database for querying

## This program reads a set of omic data and stores it in a designed SQLite database. Specified queries are performed to validate the database design. This program demonstrates the following programming fundamentals:
- Object-Oriented Programming
- Programmatic Database Access

Data come from the paper [Personal aging markers and ageotypes revealed by deep longitudinal profiling](https://www.nature.com/articles/s41591-019-0719-5), which consists of a mixture of pre-processed files produced from the measurements of the transcripts, proteins, and metabolites of a cohort of 106 individuals in the study

The database was designed to have four entities, with a one to one  relationship and two one to many relationships. 
![image](https://github.com/vincentxa847/Constructing_multi-omics_database/assets/118545004/a6a948f4-fcd3-4fc5-a115-0d3d1c0bf3bc)

For **Subject entity**, a table named Subject was created in database to store the  information from *Subject.csv*. Subject ID is used as primary key.
For Sample entity, a table named Sample was created to store the information of Sample  ID columns, Sample ID is divided into Sample ID and Visit ID (i.e. Store the visit ID  separately). 
Subject ID is stored in Sample table as a foreign key and Sample ID is utilized as primary key. 
Relationship between Subject and Sample is one to many. For example, Subject ZQMVU4Q has three Sample, which are ZQMVU4Q-01, ZQMVU4Q-03 and  ZQMVU4Q-04. Samples stored in Sample table can be referred to Subject table using 
Subject ID in Sample table. 

For **Biomolecule entity**, table called Biomolecule was created to store the biomolecule  from 
*HMP_transcriptome_abundance.tsv, HMP_proteome_abundance.tsv and HMP_metabolome_abundance.tsv*. 
Because some BiomoleculeIDs of proteomics and  transcriptomics share same name. Biomolecule ID and Omics type are set as primary 
key to better distinguish biomolecules from three data files and avoid UNIQUE  Constraint failed.

**The Relationship between Sample and Biomolecule is many to many**. A join table called Measurement is created to represent the many to many relationship. Abundance 
and Omics type are stored as relationship attributes. SampleID, BiomoleculeID and Omics type are set as primary key. 

For **Peak_annotation entity**, table called Peak_annotation was created. It only contains one column – metabolite, which stores the name of metabolite. For metabolite in many 
to one relationship such as C8:2-OH FA that has more than one cell in HMP_metabolome_annotation.csv. It will only store once in Peak_annotation table. 

**The relationship between Biomolecule and Peak annotation is many to many**, with one  biomolecule (metabolome) can have one to many annotations. 
For example, Peak  nHILIC_121.0505_3.5 has two annotations, which are Erythritol and D-Threitol.  
While others biomolecules (proteome and transcriptome) have no annotation, therefore it is optional for biomolecule entity to participate the identified relationship. 
On the other hand, a metabolite can be referred to one to many peaks. For example, Biliverdin links to nHILIC_581.2411_5.9 and pHILIC_583.2539_5.3. 
A join table called  Identification is created to represent the many to many relationships between Biomolecule and Peak annotation, and the information of metabolite is stored as 
relationship attribute in Identification. BiomoleculeID (peak) and Metabolite are set as  primary key.

## Running
The running section of program is divided into three parts, which are createdb,loaddb and querydb
First, user create database using option **createdb**, which will use *Create.sql* to build the frame of database.
```
python Parsing_Data_and_Querying.py --createdb
```
Then user load data into database using option **loaddb**,which will import the data into **2802815.db**
```
python Parsing_Data_and_Querying.py --loaddb
```
Finally, user can enter a number from 1 to 9 following option **querydb** to get the result of pre-defined queries.
If the user attempts to query the database without first creating it, the program will prompt the user to create the database. 
Similarly, if the user queries the database without inserting data, no results will be displayed on the command line.

### Queries
```
python Parsing_Data_and_Querying.py --querydb 1,2,3...
```
1.	Retrieve SubjectID and Age of subjects whose age is greater than 70.

2.	Retrieve all female SubjectID who have a healthy BMI (18.5 to 24.9). Sort the results in descending order.

3.	Retrieve the Visit IDs of Subject 'ZNQOVZV'. This query will be easy if the Visit ID information has been correctly parsed and stored into the database.

4.	Retrieve distinct SubjectIDs who have metabolomics samples and are insulin-resistant.

5.	Retrieve the unique KEGG IDs that have been annotated for the following peaks: 
a.	'nHILIC_121.0505_3.5'
b.	'nHILIC_130.0872_6.3'
c.	'nHILIC_133.0506_2.3'
d.	'nHILIC_133.0506_4.4'
This query will be easy if the annotation information has been correctly parsed and stored into the database.
6.	Retrieve the minimum, maximum and average age of Subjects.

7.	Retrieve the list of pathways from the annotation data, and the count of how many times each pathway has been annotated. Display only pathways that have a count of at least 10. Order the results by the number of annotations in descending order.

8.	Retrieve the maximum abundance of the transcript 'A1BG' for subject 'ZOZOW1T' 
across all samples.

9.	Retrieve the subjects’ age and BMI. If there are NULL values in the Age or BMI columns, that subject should be omitted from the results. 

## Requirement
Codes is tested to work under Python 3.11.1
