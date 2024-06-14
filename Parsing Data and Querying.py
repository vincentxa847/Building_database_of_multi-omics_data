import argparse
import csv
import sqlite3
from matplotlib import pyplot as plt

# specify the SQL file that are working with
print("2802815.db \n")

parser = argparse.ArgumentParser()
parser.add_argument('--createdb',required=False,action='store_true',default= False)
parser.add_argument('--loaddb',required= False,action='store_true',default= False)
parser.add_argument('--querydb',required= False, type=int)
args = parser.parse_args()

class Data:
    def __init__(self,file):
        self.file = file
    def openCsv(self):
        with open(self.file, "r") as file:
            Row = []
            content = csv.reader(file)
            for row in content:
                Row.append(row)
            return Row
    def openTsv(self):
        Row = []
        with open(self.file, "r") as file:
            content = csv.reader(file,delimiter="\t")
            for row in content:
                Row.append(row)
            return Row

class SQLOperation:
    def __init__(self,DBtoCreate): # put the name of database (string)
        self.DBtoCreate = DBtoCreate
        self.connection = sqlite3.connect(self.DBtoCreate)
        self.cursor =  self.connection.cursor()

    def create(self):
        with open("Create_Database.sql", 'r') as sql_file:
            sql_script = sql_file.read()
        db = sqlite3.connect(self.DBtoCreate)  # change the name to <SQLite database file> that specify when execute python file
        cursor = db.cursor()
        cursor.executescript(sql_script)
        db.commit()
        db.close()

    def finish(self):
        self.connection.commit()
        self.cursor.close()
        self.connection.close()

class SubjectTable:
    def __init__(self,data):
        self.data = data

    def turnToNull(self,cursor):
        for insert in self.data[1:]: # without header
            turnToNull = eval(str(insert).replace('NA', 'NULL').replace('unknown', 'NULL').replace('Unknown', 'NULL'))
            sql = f"INSERT INTO Subject VALUES ('{turnToNull[0]}','{turnToNull[1]}','{turnToNull[2]}','{(turnToNull[3])}','{(turnToNull[4])}','{(turnToNull[5])}','{(turnToNull[6])}')"
            sql = sql.replace("'NULL'", "NULL")
            try:
                sql = f"INSERT INTO Subject VALUES ('{turnToNull[0]}','{turnToNull[1]}','{turnToNull[2]}',{eval(turnToNull[3])},{eval(turnToNull[4])},{eval(turnToNull[5])},'{(turnToNull[6])}')"
                cursor.execute(sql)
            except NameError:
                try:
                    sql = f"INSERT INTO Subject VALUES ('{turnToNull[0]}','{turnToNull[1]}','{turnToNull[2]}',{turnToNull[3]},{eval(turnToNull[4])},{eval(turnToNull[5])},'{(turnToNull[6])}')"
                    cursor.execute(sql)
                except NameError:
                    try:
                        sql = f"INSERT INTO Subject VALUES ('{turnToNull[0]}','{turnToNull[1]}','{turnToNull[2]}',{turnToNull[3]},{(turnToNull[4])},{eval(turnToNull[5])},'{(turnToNull[6])}')"
                        cursor.execute(sql)
                    except NameError:
                        sql = f"INSERT INTO Subject VALUES ('{turnToNull[0]}','{turnToNull[1]}','{turnToNull[2]}',{turnToNull[3]},{(turnToNull[4])},{(turnToNull[5])},'{(turnToNull[6])}')"
                        cursor.execute(sql)

class SampleAndMeasurementAndBiomoleculeTable:
    def __init__(self,metabolome,proteome,transcriptome):
        self.metabolome = metabolome
        self.proteome = proteome
        self.transcriptome = transcriptome
        self.IDrow = [] # Store sampleID
        self.biomoleculeInMetabolome = []
        self.biomoleculeInProteome = []
        self.biomoleculeInTranscriptome = []
        self.sampleIDAndAbundanceINmetabolome = {} # store sampleID as key and corresponding Abundance as value
        self.sampleIDAndAbundanceInproteome = {}
        self.sampleIDAndAbundanceIntranscriptome = {}

    def fetch(self):
        for row in self.metabolome:
            self.IDrow.append(row[0])
            self.sampleIDAndAbundanceINmetabolome.update({row[0]:row[1:]})
        for row in self.proteome:
            self.IDrow.append(row[0])
            self.sampleIDAndAbundanceInproteome.update({row[0]: row[1:]})
        for row in self.transcriptome:
            self.IDrow.append(row[0])
            self.sampleIDAndAbundanceIntranscriptome.update({row[0]:row[1:]})
        self.IDrow = set(self.IDrow)
        self.IDrow.remove("SampleID")
        self.IDrow = list(self.IDrow)

    def insertsample(self,cursor):
        for row in self.IDrow:
            SubjectID = row[:7]
            visitID = row[8:]
            try:
                sql = f"INSERT INTO Sample VALUES ('{row}',{int(visitID)},'{SubjectID}')"
                cursor.execute(sql)
            except ValueError:
                sql = f"INSERT INTO Sample VALUES ('{row}','{visitID}','{SubjectID}')"
                cursor.execute(sql)

    # need to execute this method first, then remove row of biomolecule to insert into measurement
    def getbiomolecule(self):
        self.biomoleculeInMetabolome = self.sampleIDAndAbundanceINmetabolome.get("SampleID")  # datatype is list itself
        self.biomoleculeInProteome = self.sampleIDAndAbundanceInproteome.get("SampleID")
        self.biomoleculeInTranscriptome = self.sampleIDAndAbundanceIntranscriptome.get("SampleID")

    def removeheader(self):
        self.sampleIDAndAbundanceINmetabolome.pop("SampleID")
        self.sampleIDAndAbundanceInproteome.pop("SampleID")
        self.sampleIDAndAbundanceIntranscriptome.pop("SampleID")

    """ row is the key of sampleIDAndAbundanceINmetabolome, which is sampleID
        data is the measurements of biomolecules for each sampleID
        the order of data corresponds to the order of biomoleculeID (header)
        measurement is the cell of abundance.tsv, each sampleID will have one measurement for each biomolecule
        Some Biomolecules in transcriptome and proteome use same name(BiomoleculeID), so put addition constraint Omic type here """

    def insertMeasurement(self,cursor,Abundanceomic,BiomoleculeID,omictype):
        for row in Abundanceomic:
            data = Abundanceomic.get(row)
            counter = 0
            for Biomolecule in BiomoleculeID:
                measurement = data[counter]
                # a data corresponds to a cell in tsv file
                counter = counter + 1
                sql = f"INSERT INTO Measurement VALUES ('{row}','{Biomolecule}','{omictype}',{measurement})"
                cursor.execute(sql)

    def insertBiomolecule(self,cursor,BiomoleculeID,omictype):
        for biomolecule in BiomoleculeID:
            sql = f"INSERT INTO Biomolecule VALUES ('{biomolecule}','{omictype}')"
            cursor.execute(sql)


class AnnotationAndIdentificationTable:
    def __init__(self,annotation):
        self.annotation = annotation
        self.annotationContent = []
        self.oneToMany = [] # one to many
        self.checking = [] # store the Metabolite column (row[1]) without number
        self.OneMetaboliteToManyPeak = [] # many to one, just metabolite name
        self.manyToMany = []
        self.manyToOne = [] # not just metabolite name, but whole row
        self.oneToOne = [] # exclude onetomany,manytoone,manytomany

    def fetch(self):
        for row in self.annotation:
            self.checking.append(row[1][:-3])
            if "|" in row[1]: # one to many
                self.oneToMany.append(row)  # entire row content
                self.annotationContent.append(row)
            else: self.annotationContent.append(row)

    ''' Using below command can list all compound that rows have same metabolite with different number
        But this will select some row that is not the case, for example PI(34:1) and PI(34:2)
        So additional checking is needed to avoid truncating metabolite
        Also,OneMetaboliteToManyPeak will store same name for peak link to same metabolite. for example, Hydroxybutyric acid(2) and Hydroxybutyric acid(1) will be stored as Hydroxybutyric acid twice.
        So using set to eliminate this duplication.'''

    def ManyToOneMetabolite(self):
        OneMetaboliteToManyPeakCheck = {same for same in self.checking if self.checking.count(same) > 1}  # just metabolite name
        for row in self.annotationContent:
            for metabolite in OneMetaboliteToManyPeakCheck :
                if metabolite in row[1][:-3]:
                    if row[1].rfind("(") == len(metabolite):
                        self.OneMetaboliteToManyPeak.append(metabolite)
        self.OneMetaboliteToManyPeak = set(self.OneMetaboliteToManyPeak)

    def ManyToMany(self):
        for row in self.annotationContent:
            for onetomany in self.oneToMany:  # whole row
                for manytoone in self.OneMetaboliteToManyPeak :  # just metabolite name
                    if row == onetomany and manytoone in row[1]:  # many to many relationship
                        self.manyToMany.append(row)

    def ManyToOne(self):
        for metabolite in self.OneMetaboliteToManyPeak:
            for row in self.annotationContent:
                if metabolite in row[1]:
                    self.manyToOne.append(row)

    def OnetoOne(self):
        self.manyToOne = [row for row in self.manyToOne if row not in self.manyToMany] # remove many to many
        self.oneToMany = [row for row in self.oneToMany if row not in self.manyToMany] # remove many to many
        self.oneToOne = [row for row in self.annotationContent if row not in (self.manyToMany + self.manyToOne + self.oneToMany)]

    def insertManyToMany(self,cursor):
        for row in self.manyToMany:
            for character in range(len(row[1])):
                counter = row[1][character]
                if counter == "|":
                    firstMetabolite = row[1][:character]
                    secondMetabolite = row[1][character +2 :-3]
                    break
            try:
                sql = f"INSERT INTO Peak_annotation VALUES ('{firstMetabolite}')"
                cursor.execute(sql)
                sql = f"INSERT INTO Peak_annotation VALUES ('{secondMetabolite}')"
                cursor.execute(sql)
            except sqlite3.IntegrityError:
                pass
            break

    def insertOneToMany(self,cursor):
        for row in self.oneToMany:
            for character in range(len(row[1])):
                counter = row[1][character]
                if counter == "|":
                    firstMetabolite = row[1][:character]
                    secondMetabolite = row[1][character + 1:]
                    break
            try:
                sql = f"INSERT INTO Peak_annotation VALUES ('{firstMetabolite}')"
                cursor.execute(sql)
                sql = f"INSERT INTO Peak_annotation VALUES ('{secondMetabolite}')"
                cursor.execute(sql)
            except sqlite3.IntegrityError:
                pass

    def insertManyToOne(self,cursor):
        for row in self.manyToOne:
            try:
                sql = f"INSERT INTO Peak_annotation VALUES ('{row[1][:-3]}')"
                cursor.execute(sql)
            except sqlite3.IntegrityError:
                pass

    def insertOneToOne(self,cursor):
        for row in self.oneToOne:
            try:
                sql = f"INSERT INTO Peak_annotation VALUES ('{row[1]}')"
                cursor.execute(sql)
            except sqlite3.IntegrityError:
                pass

    def identificationManyToMany(self,cursor):
        for row in self.manyToMany:
            for character in range(len(row[1])):
                counter = row[1][character]
                if counter == "|":
                    ManytoManyfirstMetabolite = row[1][:character].rstrip()
                    ManytoManysecondMetabolite = row[1][character + 2:-3].rstrip()
                    break
            sql1 = f"INSERT INTO Identification VALUES ('{row[0]}','{ManytoManyfirstMetabolite}','{row[2]}','{row[3]}','{row[4]}','{row[5]}')"
            cursor.execute(sql1)
            sql2 = f"INSERT INTO Identification VALUES ('{row[0]}','{ManytoManysecondMetabolite}','{row[2]}','{row[3]}','{row[4]}','{row[5]}')"
            cursor.execute(sql2)

    def identificationOneToMany(self,cursor):
        for row in self.oneToMany:
            for character in range(len(row[1])):
                counter = row[1][character]
                if counter == "|":
                    firstMetabolite = row[1][:character].rstrip()
                    secondMetabolite = row[1][character + 1:].rstrip()
                    break
            if row[2] != "":
                for character in range(len(row[2])):
                    counter = row[2][character]
                    if counter == "|":
                        firstKEGG = row[2][:character].rstrip()
                        secondKEGG = row[2][character + 1:].rstrip()
                        break
            else:
                firstKEGG = ""
                secondKEGG = ""

            if row[3] != "":
                for character in range(len(row[3])):
                    counter = row[3][character]
                    if counter == "|":
                        firstHMDB = row[3][:character].rstrip()
                        secondHMDB = row[3][character + 1:].rstrip()
                        break
            else:
                firstHMDB = ""
                secondHMDB = ""

            sql = f"INSERT INTO Identification VALUES ('{row[0]}','{firstMetabolite}','{firstKEGG}','{firstHMDB}','{row[4]}','{row[5]}')"
            cursor.execute(sql)
            sql = f"INSERT INTO Identification VALUES ('{row[0]}','{secondMetabolite}','{secondKEGG}','{secondHMDB}','{row[4]}','{row[5]}')"
            cursor.execute(sql)

    def identificationManyToOne(self,cursor):
        for row in self.manyToOne:
            sql = f"INSERT INTO Identification VALUES ('{row[0]}','{row[1][:-3]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}')"
            cursor.execute(sql)

    def identificationOneToOne(self,cursor):
        for row in self.oneToOne:
            sql = f"INSERT INTO Identification VALUES ('{row[0]}','{row[1]}','{row[2]}','{row[3]}','{row[4]}','{row[5]}')"
            cursor.execute(sql)

def querying(db,sql):
    connection = sqlite3.connect(db)
    cur = connection.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    connection.close()
    return rows

if args.createdb:
    # CreateDatabase
    Database = SQLOperation("2802815.db")
    Database.create()

if args.loaddb:
    try:
        Database = SQLOperation("2802815.db")
        # Import data
        Subject = Data('Subject.csv')
        Metabolome = Data('HMP_metabolome_abundance.tsv')
        Proteome = Data('HMP_proteome_abundance.tsv')
        Transcriptome = Data('HMP_transcriptome_abundance.tsv')
        Annotation = Data('HMP_metabolome_annotation.csv')

        SubjectContent = Subject.openCsv()
        MetabolomeContent = Metabolome.openTsv()
        ProteomeContent = Proteome.openTsv()
        TranscriptomeContent = Transcriptome.openTsv()
        AnnotationContent = Annotation.openCsv()
        # INSERT INTO Subject Table
        InsertSubject = SubjectTable(SubjectContent)
        InsertSubject.turnToNull(Database.cursor)

        # INSERT INTO Sample Table
        InsertSampleAndMeasurement = SampleAndMeasurementAndBiomoleculeTable(MetabolomeContent,ProteomeContent,TranscriptomeContent)
        InsertSampleAndMeasurement.fetch()
        InsertSampleAndMeasurement.insertsample(Database.cursor)

        # INSERT INTO Measurement Table
        InsertSampleAndMeasurement.getbiomolecule() # update the self.biomolecule
        InsertSampleAndMeasurement.removeheader() # remove the biomolecule for insertion of Measurement
        # INSERT THE METABOLOME DATA INTO TABLE
        InsertSampleAndMeasurement.insertMeasurement(Database.cursor,InsertSampleAndMeasurement.sampleIDAndAbundanceINmetabolome,InsertSampleAndMeasurement.biomoleculeInMetabolome,'Metabolome')
        # INSERT THE PROTEOME DATA INTO TABLE
        InsertSampleAndMeasurement.insertMeasurement(Database.cursor,InsertSampleAndMeasurement.sampleIDAndAbundanceInproteome,InsertSampleAndMeasurement.biomoleculeInProteome,'Proteome')
        # INSERT THE TRANSCRIPTOME DATA INTO TABLE
        InsertSampleAndMeasurement.insertMeasurement(Database.cursor,InsertSampleAndMeasurement.sampleIDAndAbundanceIntranscriptome,InsertSampleAndMeasurement.biomoleculeInTranscriptome,'Transcriptome')
        # INSERT INTO Biomolecule TABLE
        InsertSampleAndMeasurement.insertBiomolecule(Database.cursor,InsertSampleAndMeasurement.biomoleculeInMetabolome,'Metabolome')
        InsertSampleAndMeasurement.insertBiomolecule(Database.cursor,InsertSampleAndMeasurement.biomoleculeInProteome,'Proteome')
        InsertSampleAndMeasurement.insertBiomolecule(Database.cursor,InsertSampleAndMeasurement.biomoleculeInTranscriptome,'Transcriptome')

        # INSERT INTO Peak_annotation TABLE
        InsertAnnotation = AnnotationAndIdentificationTable(AnnotationContent)
        InsertAnnotation.fetch()
        InsertAnnotation.ManyToOneMetabolite()
        InsertAnnotation.ManyToMany()
        InsertAnnotation.ManyToOne()
        InsertAnnotation.OnetoOne()
        InsertAnnotation.insertOneToMany(Database.cursor)
        InsertAnnotation.insertManyToMany(Database.cursor)
        InsertAnnotation.insertOneToMany(Database.cursor)
        InsertAnnotation.insertManyToOne(Database.cursor)
        InsertAnnotation.insertOneToOne(Database.cursor)
        # INSERT INTO Measurement TABLE
        InsertAnnotation.identificationManyToMany(Database.cursor)
        InsertAnnotation.identificationOneToMany(Database.cursor)
        InsertAnnotation.identificationManyToOne(Database.cursor)
        InsertAnnotation.identificationOneToOne(Database.cursor)

        Database.finish()

    except sqlite3.OperationalError:
        print("need to create database first")

try:
    if args.querydb == 1:
        sql = "SELECT Subject.SubjectID, Subject.Age FROM Subject WHERE Age>70"
        result = querying("2802815.db",sql)
        print("SubjectID \t Age")
        for row in result:
            ID,Age = row
            print(f"{ID} \t {Age}")

    if args.querydb == 2:
        sql = """SELECT Subject.SubjectID FROM Subject WHERE Sex like "F" AND BMI BETWEEN 18.5 AND 24.9 
        ORDER BY SubjectID DESC"""
        result = querying("2802815.db",sql)
        for row in result:
            print(f"{row[0]}")


    if args.querydb == 3:
        sql = "SELECT Visit_ID FROM Sample WHERE Sample.SubjectID = 'ZNQOVZV'"
        result = querying("2802815.db", sql)
        print("Visit_ID")
        for row in result:
            Visit_ID= row
            print(Visit_ID[0])


    if args.querydb == 4:
        sql = """SELECT DISTINCT Subject.SubjectID
        FROM Subject,Sample,Measurement
        WHERE Subject.SubjectID = Sample.SubjectID AND
        Sample.SampleID = Measurement.SampleID
          AND Subject.IRIRclassification = 'IR'
          AND EXISTS ( SELECT Measurement.Omics_type
                       FROM Measurement
                       WHERE Measurement.Omics_type = 'Transcriptome'
                       )"""
        result = querying("2802815.db", sql)
        print("SubjectID")
        for row in result:
            SubjectID= row
            print(SubjectID[0])

    if args.querydb == 5:
        sql = """ SELECT Identification.BiomoleculeID, Identification.KEGG
        FROM Identification
        WHERE Identification.BiomoleculeID = "nHILIC_121.0505_3.5" OR Identification.BiomoleculeID = "nHILIC_130.0872_6.3"
        OR Identification.BiomoleculeID = "nHILIC_133.0506_2.3" OR Identification.BiomoleculeID = "nHILIC_133.0506_4.4" """
        result = querying("Test7.db", sql)
        print("BiomoleculeID \t \t KEGG")
        for row in result:
            ID,KEGG = row
            print(f"{ID} \t {KEGG}")

    if args.querydb == 6:
        sql = "SELECT MAX(Subject.Age) AS 'maximum', MIN(Subject.Age) AS 'minimum' , AVG(Subject.Age) AS 'average ' FROM Subject"
        result = querying("2802815.db", sql)
        print("MAX \t MIN \t AVG")
        for row in result:
            MAX,MIN,AVG= row
            print(f"{MAX} \t {MIN} \t {AVG}")


    if args.querydb == 7:
        sql = """ SELECT Identification.Pathway, count(*)
        FROM Identification
        GROUP BY Identification.Pathway
        HAVING count(*) > 10
        ORDER BY count(*) DESC """
        result = querying("2802815.db", sql)
        print("Pathway \t count")
        for row in result:
            Pathway,count = row
            print(f"{Pathway} \t {count}")

    if args.querydb == 8:
        sql = """ SELECT MAX(Measurement.Abundance)
        FROM Sample, Measurement
        WHERE Sample.SampleID = Measurement.SampleID AND Sample.SubjectID = "ZOZOW1T" AND Measurement.BiomoleculeID = "A1BG" AND Measurement.Omics_type = "Transcriptome" """
        result = querying("2802815.db", sql)
        print("MAX_Abundance")
        for row in result:
            MAX = row
            print(MAX[0])


    if args.querydb == 9:
        xaxis = []
        yaxis = []
        sql = """ SELECT Subject.Age, Subject.BMI
        FROM Subject
        WHERE Subject.Age IS NOT NULL AND Subject.BMI IS NOT NULL """
        result = querying("2802815.db", sql)
        print("Age \t BMI")
        for row in result:
            Age,BMI = row
            xaxis.append(Age)
            yaxis.append(BMI)
            print(f"{Age} \t {BMI}")
        fig, ax = plt.subplots()
        ax.scatter(xaxis, yaxis, label='2802815')
        fig.legend()
        fig.savefig('age_bmi_scatterplot.png')
        plt.clf()

except sqlite3.OperationalError:
    print("need to create database first")