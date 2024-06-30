-- Data Cleaning


SELECT * 
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT INTO layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH duplicates_cte AS
(
SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicates_cte
WHERE row_num > 1;

SELECT * 
FROM layoffs_staging
WHERE company = 'casper';

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;


INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER () OVER (PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, 
`date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

SELECT * 
FROM layoffs_staging2;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

SELECT DISTINCT(company)
FROM layoffs_staging2;

SELECT TRIM(company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT DISTINCT(Company)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT DISTINCT (industry)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET Country = TRIM(TRAILING '.' FROM Country)
WHERE Country LIKE 'United States';

SELECT *
FROM layoffs_staging2
WHERE country = 'United States';

SELECT `date`,
STR_TO_DATE (`date`, '%m/%d/%Y')
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE (`date`, '%m/%d/%Y');


SELECT *
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM layoffs_staging2
WHERE company = 'airbnb';

UPDATE layoffs_staging2
SET industry = NULL
WHERE industry = '';

SELECT  t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1;

SELECT company, MAX(total_laid_off)
FROM layoffs_staging2
GROUP BY company;

-- Data Exploration

SELECT *
FROM layoffs_staging2;

SELECT Country, MAX(total_laid_off) AS highest_laid_off
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY Country
ORDER BY 2 DESC;

SELECT SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2;

-- Total layoffs 38,3659

SELECT Company, SUM(total_laid_off) AS Total_layoffs
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY company
ORDER BY 2 DESC;


SELECT DISTINCT Industry, SUM(total_laid_off)
FROM layoffs_staging2
WHERE total_laid_off IS NOT NULL
GROUP BY Industry, total_laid_off
ORDER BY 2 DESC;

SELECT AVG(total_laid_off)
FROM layoffs_staging2;



SELECT country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT YEAR(`date`), SUM(total_laid_off)
FROM layoffs_staging2
WHERE YEAR(`date`) IS NOT NULL
GROUP BY YEAR(`date`)
ORDER BY 1;

SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1;

WITH Rolling_total AS
(
SELECT SUBSTRING(`date`,1,7) AS `Month`, SUM(total_laid_off) AS Total
FROM layoffs_staging2
WHERE SUBSTRING(`date`,1,7) IS NOT NULL
GROUP BY `Month`
ORDER BY 1
)
SELECT `Month`, Total, SUM(Total) OVER(ORDER BY `Month`) AS rolling_total
FROM Rolling_total;
 
 WITH Company_Year (company, years, total_laid_off) AS 
 (
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
ORDER BY 3 DESC
 ), Company_Year_Rank AS
 (
SELECT *, 
DENSE_RANK () OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM Company_Year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5;

SELECT *
FROM layoffs_staging2;

SELECT industry, SUM(total_laid_off)
FROM layoffs_staging2
WHERE industry IS NOT NULL
GROUP BY industry
ORDER BY 2 DESC;

SELECT company, SUM(funds_raised_millions) AS total_funds_raised
FROM layoffs_staging2
GROUP BY company
HAVING SUM(funds_raised_millions) IS NOT NULL
ORDER BY 2 DESC;












