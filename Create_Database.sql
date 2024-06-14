CREATE TABLE Subject (
	SubjectID CHAR(7) NOT NULL,
	Race CHAR(1),
	Sex CHAR(1),
	Age DECIMAL(3,2),
	BMI DECIMAL(2,2),
	SSPG DECIMAL(3,3),
	IRIRclassification CHAR(2),
	PRIMARY KEY (SubjectID)
	);

CREATE TABLE Sample (
	SampleID VARCHAR(13) NOT NULL,
	Visit_ID INT,
	SubjectID CHAR(7),
	PRIMARY KEY (SampleID),
	FOREIGN KEY (SubjectID) REFERENCES Subject(SubjectID)
	);

CREATE TABLE Biomolecule (
	BiomoleculeID VARCHAR(25) NOT NULL,
	Omics_type VARCHAR(15),
	PRIMARY KEY (BiomoleculeID,Omics_type)
	);

CREATE TABLE Peak_annotation (
	Metabolite VARCHAR(35) NOT NULL,
	PRIMARY KEY (Metabolite)
	);

-- JOIN TABLE FOR MEASURES RELATIONSHIP BETWEEN SAMPLE AND BIOMOLECULE
CREATE TABLE Measurement (
	SampleID VARCHAR(13) NOT NULL,
	BiomoleculeID VARCHAR(25) NOT NULL,
	Omics_type VARCHAR(15) NOT NULL, -- proteome and metabolome have same name for some biomoleculeID
	Abundance DECIMAL(11,11), -- decimal can store negative number
	PRIMARY KEY(SampleID,BiomoleculeID,Omics_type),
	FOREIGN KEY (SampleID) REFERENCES Sample(SampleID),
	FOREIGN KEY (BiomoleculeID) REFERENCES Biomolecule(BiomoleculeID)
	);


-- JOIN TABLE FOR IDENTIFIED RELATIONSHIP BETWEEN BIOMOLEULE AND PEAK_ANNOTATION
CREATE TABLE Identification (
	BiomoleculeID VARCHAR(25) NOT NULL,
	Metabolite VARCHAR(35) NOT NULL,
	KEGG CHAR(6),
	HMDB CHAR(9),
	ChemicalClass VARCHAR(15),
	Pathway VARCHAR (50),
	PRIMARY KEY (BiomoleculeID,Metabolite),
	FOREIGN KEY (BiomoleculeID) REFERENCES Biomolecule(BiomoleculeID),
	FOREIGN KEY (Metabolite) REFERENCES Peak_annotation(Metabolite)
	);