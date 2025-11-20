CREATE TABLE "customers-100" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "customers-100_phone_list" (
  "Index" INTEGER,
  "orig_col" TEXT,
  "phone" TEXT
);

CREATE TABLE "Customer Id_dim" (
  "Customer Id" TEXT,
  "City" TEXT,
  "Company" TEXT,
  "Country" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Last Name" TEXT,
  "Phone 1" TEXT,
  "Phone 2" TEXT,
  "Subscription Date" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Customer Id")
);

CREATE TABLE "City_dim" (
  "City" TEXT,
  "Company" TEXT,
  "Country" TEXT,
  "Customer Id" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Last Name" TEXT,
  "Phone 1" TEXT,
  "Phone 2" TEXT,
  "Subscription Date" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("City")
);

CREATE TABLE "Phone 1_dim" (
  "Phone 1" TEXT,
  "City" TEXT,
  "Company" TEXT,
  "Country" TEXT,
  "Customer Id" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Last Name" TEXT,
  "Phone 2" TEXT,
  "Subscription Date" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Phone 1")
);

CREATE TABLE "Phone 2_dim" (
  "Phone 2" TEXT,
  "City" TEXT,
  "Company" TEXT,
  "Country" TEXT,
  "Customer Id" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Last Name" TEXT,
  "Phone 1" TEXT,
  "Subscription Date" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Phone 2")
);

CREATE TABLE "Email_dim" (
  "Email" TEXT,
  "Date of birth" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Job Title" TEXT,
  "Last Name" TEXT,
  "Phone" TEXT,
  "Sex" TEXT,
  "User Id" TEXT,
  PRIMARY KEY ("Email")
);

CREATE TABLE "Website_dim" (
  "Website" TEXT,
  "Country" TEXT,
  "Description" TEXT,
  "Founded" INTEGER,
  "Index" INTEGER,
  "Industry" TEXT,
  "Name" TEXT,
  "Number of employees" INTEGER,
  "Organization Id" TEXT,
  PRIMARY KEY ("Website")
);

CREATE TABLE "customers-1000" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "customers-1000_phone_list" (
  "Index" INTEGER,
  "orig_col" TEXT,
  "phone" TEXT
);

CREATE TABLE "customers-10000" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "customers-10000_phone_list" (
  "Index" INTEGER,
  "orig_col" TEXT,
  "phone" TEXT
);

CREATE TABLE "organizations-100" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "Organization Id_dim" (
  "Organization Id" TEXT,
  "Country" TEXT,
  "Description" TEXT,
  "Founded" INTEGER,
  "Index" INTEGER,
  "Industry" TEXT,
  "Name" TEXT,
  "Number of employees" INTEGER,
  "Website" TEXT,
  PRIMARY KEY ("Organization Id")
);

CREATE TABLE "Name_dim" (
  "Name" TEXT,
  "Country" TEXT,
  "Description" TEXT,
  "Founded" INTEGER,
  "Index" INTEGER,
  "Industry" TEXT,
  "Number of employees" INTEGER,
  "Organization Id" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Name")
);

CREATE TABLE "Description_dim" (
  "Description" TEXT,
  "Country" TEXT,
  "Founded" INTEGER,
  "Index" INTEGER,
  "Industry" TEXT,
  "Name" TEXT,
  "Number of employees" INTEGER,
  "Organization Id" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Description")
);

CREATE TABLE "Number of employees_dim" (
  "Number of employees" INTEGER,
  "Country" TEXT,
  "Description" TEXT,
  "Founded" INTEGER,
  "Index" INTEGER,
  "Industry" TEXT,
  "Name" TEXT,
  "Organization Id" TEXT,
  "Website" TEXT,
  PRIMARY KEY ("Number of employees")
);

CREATE TABLE "organizations-1000" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "people-100" (
  PRIMARY KEY ("Index")
);

CREATE TABLE "User Id_dim" (
  "User Id" TEXT,
  "Date of birth" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Job Title" TEXT,
  "Last Name" TEXT,
  "Phone" TEXT,
  "Sex" TEXT,
  PRIMARY KEY ("User Id")
);

CREATE TABLE "Phone_dim" (
  "Phone" TEXT,
  "Date of birth" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Job Title" TEXT,
  "Last Name" TEXT,
  "Sex" TEXT,
  "User Id" TEXT,
  PRIMARY KEY ("Phone")
);

CREATE TABLE "Date of birth_dim" (
  "Date of birth" TEXT,
  "Email" TEXT,
  "First Name" TEXT,
  "Index" INTEGER,
  "Job Title" TEXT,
  "Last Name" TEXT,
  "Phone" TEXT,
  "Sex" TEXT,
  "User Id" TEXT,
  PRIMARY KEY ("Date of birth")
);

CREATE TABLE "people-1000" (
  PRIMARY KEY ("Index")
);

ALTER TABLE "customers-100_phone_list" ADD CONSTRAINT "fk_customers-100_phone_list_Index_customers-100" FOREIGN KEY ("Index") REFERENCES "customers-100" ("Index");

ALTER TABLE "customers-1000_phone_list" ADD CONSTRAINT "fk_customers-1000_phone_list_Index_customers-1000" FOREIGN KEY ("Index") REFERENCES "customers-1000" ("Index");

ALTER TABLE "customers-10000_phone_list" ADD CONSTRAINT "fk_customers-10000_phone_list_Index_customers-10000" FOREIGN KEY ("Index") REFERENCES "customers-10000" ("Index");

CREATE INDEX "ix_customers-100_phone_list_Index" ON "customers-100_phone_list" ("Index");

CREATE INDEX "ix_customers-1000_phone_list_Index" ON "customers-1000_phone_list" ("Index");

CREATE INDEX "ix_customers-10000_phone_list_Index" ON "customers-10000_phone_list" ("Index");
