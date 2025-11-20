-- Unpivot phone columns from customers-100_phone_list into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, phone AS PhoneNumber, 'phone' AS PhoneType FROM "customers-100_phone_list" WHERE phone IS NOT NULL;

-- Populate Company from Customer Id_dim
INSERT INTO Company (CompanyName, Website) SELECT DISTINCT Company, Website FROM "Customer Id_dim" WHERE Company IS NOT NULL;

-- Populate Location from Customer Id_dim
INSERT INTO Location (City, Country) SELECT DISTINCT City, Country FROM "Customer Id_dim" WHERE City IS NOT NULL OR Country IS NOT NULL;

-- Populate Customer/Person-like entities from Customer Id_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, Subscription Date, NULL, NULL FROM "Customer Id_dim" WHERE 1=1;

-- Unpivot phone columns from Customer Id_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT Customer Id AS CustomerId, Phone 1 AS PhoneNumber, 'Phone 1' AS PhoneType FROM "Customer Id_dim" WHERE Phone 1 IS NOT NULL
UNION ALL
SELECT Customer Id AS CustomerId, Phone 2 AS PhoneNumber, 'Phone 2' AS PhoneType FROM "Customer Id_dim" WHERE Phone 2 IS NOT NULL;

-- Populate Company from City_dim
INSERT INTO Company (CompanyName, Website) SELECT DISTINCT Company, Website FROM "City_dim" WHERE Company IS NOT NULL;

-- Populate Location from City_dim
INSERT INTO Location (City, Country) SELECT DISTINCT City, Country FROM "City_dim" WHERE City IS NOT NULL OR Country IS NOT NULL;

-- Populate Customer/Person-like entities from City_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, Subscription Date, NULL, NULL FROM "City_dim" WHERE 1=1;

-- Unpivot phone columns from City_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT Customer Id AS CustomerId, Phone 1 AS PhoneNumber, 'Phone 1' AS PhoneType FROM "City_dim" WHERE Phone 1 IS NOT NULL
UNION ALL
SELECT Customer Id AS CustomerId, Phone 2 AS PhoneNumber, 'Phone 2' AS PhoneType FROM "City_dim" WHERE Phone 2 IS NOT NULL;

-- Populate Company from Phone 1_dim
INSERT INTO Company (CompanyName, Website) SELECT DISTINCT Company, Website FROM "Phone 1_dim" WHERE Company IS NOT NULL;

-- Populate Location from Phone 1_dim
INSERT INTO Location (City, Country) SELECT DISTINCT City, Country FROM "Phone 1_dim" WHERE City IS NOT NULL OR Country IS NOT NULL;

-- Populate Customer/Person-like entities from Phone 1_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, Subscription Date, NULL, NULL FROM "Phone 1_dim" WHERE 1=1;

-- Unpivot phone columns from Phone 1_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT Customer Id AS CustomerId, Phone 1 AS PhoneNumber, 'Phone 1' AS PhoneType FROM "Phone 1_dim" WHERE Phone 1 IS NOT NULL
UNION ALL
SELECT Customer Id AS CustomerId, Phone 2 AS PhoneNumber, 'Phone 2' AS PhoneType FROM "Phone 1_dim" WHERE Phone 2 IS NOT NULL;

-- Populate Company from Phone 2_dim
INSERT INTO Company (CompanyName, Website) SELECT DISTINCT Company, Website FROM "Phone 2_dim" WHERE Company IS NOT NULL;

-- Populate Location from Phone 2_dim
INSERT INTO Location (City, Country) SELECT DISTINCT City, Country FROM "Phone 2_dim" WHERE City IS NOT NULL OR Country IS NOT NULL;

-- Populate Customer/Person-like entities from Phone 2_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, Subscription Date, NULL, NULL FROM "Phone 2_dim" WHERE 1=1;

-- Unpivot phone columns from Phone 2_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT Customer Id AS CustomerId, Phone 2 AS PhoneNumber, 'Phone 2' AS PhoneType FROM "Phone 2_dim" WHERE Phone 2 IS NOT NULL
UNION ALL
SELECT Customer Id AS CustomerId, Phone 1 AS PhoneNumber, 'Phone 1' AS PhoneType FROM "Phone 2_dim" WHERE Phone 1 IS NOT NULL;

-- Populate Customer/Person-like entities from Email_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, NULL, NULL, NULL FROM "Email_dim" WHERE 1=1;

-- Unpivot phone columns from Email_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, Phone AS PhoneNumber, 'Phone' AS PhoneType FROM "Email_dim" WHERE Phone IS NOT NULL;

-- Unpivot phone columns from customers-1000_phone_list into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, phone AS PhoneNumber, 'phone' AS PhoneType FROM "customers-1000_phone_list" WHERE phone IS NOT NULL;

-- Unpivot phone columns from customers-10000_phone_list into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, phone AS PhoneNumber, 'phone' AS PhoneType FROM "customers-10000_phone_list" WHERE phone IS NOT NULL;

-- Populate Customer/Person-like entities from User Id_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, NULL, NULL, NULL FROM "User Id_dim" WHERE 1=1;

-- Unpivot phone columns from User Id_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, Phone AS PhoneNumber, 'Phone' AS PhoneType FROM "User Id_dim" WHERE Phone IS NOT NULL;

-- Populate Customer/Person-like entities from Phone_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, NULL, NULL, NULL FROM "Phone_dim" WHERE 1=1;

-- Unpivot phone columns from Phone_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, Phone AS PhoneNumber, 'Phone' AS PhoneType FROM "Phone_dim" WHERE Phone IS NOT NULL;

-- Populate Customer/Person-like entities from Date of birth_dim
-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders
INSERT INTO Customer (CustomerId, FirstName, LastName, Email, SubscriptionDate, CompanyId, LocationId) SELECT First Name, Last Name, Email, NULL, NULL, NULL FROM "Date of birth_dim" WHERE 1=1;

-- Unpivot phone columns from Date of birth_dim into Phone
INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) SELECT NULL AS CustomerId, Phone AS PhoneNumber, 'Phone' AS PhoneType FROM "Date of birth_dim" WHERE Phone IS NOT NULL;

