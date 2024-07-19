-- Data Cleaning PROJECT MYSQL

-- 1. Creating another copy table to work on 

CREATE TABLE layoff_copy1 LIKE layoffs;
INSERT INTO layoff_copy1 
SELECT * FROM layoffs;

SELECT * FROM layoff_copy1;

-- 2. Remove Duplicates

WITH duplicates_cte AS 
(
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoff_copy1                                    -- This will assign 2 for every row which repeats 
)
SELECT * FROM duplicates_cte WHERE row_num > 1;       -- Extracting all of the duplicate rows 


CREATE TABLE `layoff_copy2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int                                       -- creating another table similar to layoff_copy1 but with an additional column row_num
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci; 

INSERT INTO layoff_copy2
SELECT * ,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, 
total_laid_off, percentage_laid_off, `date`, 
stage, country, funds_raised_millions) AS row_num
FROM layoff_copy1;

DELETE 
FROM layoff_copy2 where row_num > 1 ;
SELECT *
FROM layoff_copy2 where row_num > 1 ;

-- 3. Standardize the Data : Finding issues in data and fix it 

UPDATE layoff_copy2
SET company = TRIM(company);  -- removing white spaces

SELECT DISTINCT industry 
FROM layoff_copy2
ORDER BY 1;   
-- FINDINGS :crypto, crypto currency and crytocurrency are same 

UPDATE layoff_copy2
SET industry = 'crypto'
WHERE industry LIKE 'crypto%';

SELECT DISTINCT location 
FROM layoff_copy2
ORDER BY 1;          -- no problem 

SELECT DISTINCT country
FROM layoff_copy2
ORDER BY 1;   -- United States & United States. two distinct country but they are not 

UPDATE layoff_copy2
SET country = TRIM( TRAILING '.' FROM country)  -- TRAILING removes the . from last 
WHERE country LIKE 'United States%';  

-- Fixing the data type it's text right now we have to change it to date datatype 

SELECT `date` , STR_TO_DATE(`date`, '%m/%d/%Y')  
FROM layoff_copy2; 

UPDATE layoff_copy2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y') ; -- this will convert into date format only not into date datatype

ALTER TABLE layoff_copy2
MODIFY COLUMN `date` DATE;  -- this would change the data type from text to date

-- 3. Null Values or blank Values 
-- Working on null values and blank cells missing values

SELECT * 
FROM layoff_copy2
WHERE industry is NULL OR industry = '';

UPDATE layoff_copy2
SET industry = NULL
Where industry = '';  -- replacing all the blank space with null

SELECT t1.industry, t2.industry
FROM layoff_copy2 AS t1
JOIN layoff_copy2 AS t2
     ON t1.company = t2.company
WHERE (t1.industry IS NULL ) 
AND t2.industry IS NOT NULL;  
-- this will give the blank cell industry cell which have a industry name somewhere else for the same company



UPDATE layoff_copy2 AS t1
JOIN layoff_copy2 AS t2
     ON t1.company = t2.company
     SET t1.industry = t2.industry
WHERE (t1.industry IS NULL) 
AND t2.industry IS NOT NULL;  

SELECT * FROM layoff_copy2 WHERE industry IS NULL OR industry = ''; 
/* This gives one more entry of 
company bally which have null value*/

SELECT * FROM layoff_copy2 WHERE company = "Bally's Interactive";
/* because we don't have any past info about the company industry we will delete this */


-- 4. Removing Column & Rows which can't be fixed due to data constraint

DELETE FROM layoff_copy2 WHERE company = "Bally's Interactive";

SELECT * 
FROM  layoff_copy2
WHERE total_laid_off IS NULL AND 
percentage_laid_off IS NULL;  
-- we don't have any info to fill the null values so it's better to get rid of these rows 
DELETE
FROM  layoff_copy2
WHERE total_laid_off IS NULL AND 
percentage_laid_off IS NULL;  

ALTER TABLE layoff_copy2 DROP COLUMN row_num; -- Deleting the additional column we made intially 

SELECT * FROM layoff_copy2;


-- Exploratory Data Analysis 

SELECT MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoff_copy2;
-- MAX total laid off is 12000 in a day 
-- MAX percentage laid off in a day is 100% that means entire employees are laid off company got closed maybe

SELECT count(company)
FROM layoff_copy2
WHERE percentage_laid_off = 1;
-- 116 companies have 100% laid off in a day

SELECT company, sum(total_laid_off)
FROM layoff_copy2
GROUP BY company
ORDER BY 2 DESC;

SELECT MIN(`date`), MAX(`date`)
FROM layoff_copy2;
-- data we have between 2020-03-11 and  2023-03-06  -- since the time of covid

SELECT industry, sum(total_laid_off)
FROM layoff_copy2
GROUP BY industry
ORDER BY 2 DESC;
-- companies in consumer industry & Retail got most of the laid off

SELECT country, sum(total_laid_off)
FROM layoff_copy2
GROUP BY country
ORDER BY 2 DESC;
-- united states & india got the highest laid off since 2020

SELECT YEAR(`date`) , sum(total_laid_off)
FROM layoff_copy2
GROUP BY YEAR(`date`)
ORDER BY 1 DESC;
/*IN 2022 most people got laid off from their 
job but cause we have only 3 months of data for 
2023 and the number is 125k already therefore 2023 
will be the worst year*/

SELECT stage, sum(total_laid_off)
FROM layoff_copy2
GROUP BY stage
ORDER BY 2 DESC;

WITH ROLLING_CTE AS
(
SELECT SUBSTRING(`date`, 1,7) AS `MONTH`, 
	   SUM(total_laid_off) as total_OFF
FROM layoff_copy2
WHERE SUBSTRING(`date`, 1,7) IS NOT NULL
GROUP BY 1
ORDER BY 1 ASC
)

SELECT `MONTH`, total_OFF, sum(total_OFF) OVER (ORDER BY `MONTH`)
FROM ROLLING_CTE;

SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_copy2
GROUP BY 1,2
ORDER BY 1 ASC;

WITH Company_year (company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off)
FROM layoff_copy2
GROUP BY 1,2
), 
COMPANY_Year_Rank AS 
(
SELECT *, 
DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS RANKING
FROM Company_year
WHERE years IS NOT NULL
)
Select * from COMPANY_Year_Rank
where Ranking <=5;


