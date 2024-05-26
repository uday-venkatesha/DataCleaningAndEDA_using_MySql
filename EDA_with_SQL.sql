-- EDA

-- Here we are jsut going to explore the data and find trends or patterns or anything interesting like outliers

SELECT * 
FROM world_layoffs.layoffs_staging2;


SELECT MAX(total_laid_off)
FROM world_layoffs.layoffs_staging2;

-- Looking at Percentage to see how big these layoffs were
SELECT MAX(percentage_laid_off),  MIN(percentage_laid_off)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off IS NOT NULL;

-- Which companies had 1 which is basically 100 percent of they company laid off
SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;

SELECT count(*)
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1;


-- these are mostly startups it looks like who all went out of business during this time

-- if we order by funcs_raised_millions we can see how big some of these companies were


-- funds_raised_millions column was in text format so we need to change it to INT. 


-- this query updates the row to null which is not an integer 
update layoffs_staging2 
SET funds_raised_millions = NULL
WHERE funds_raised_millions IS NOT NULL AND funds_raised_millions NOT REGEXP '^-?[0-9]+$';

-- this casts the funds_raised_millions column to unsigned so that we can convert it in alter query
UPDATE layoffs_staging2
SET funds_raised_millions = CAST(funds_raised_millions AS UNSIGNED)
WHERE funds_raised_millions IS NOT NULL;
-- this alters the column to Integer type.
ALTER TABLE layoffs_staging2  MODIFY COLUMN funds_raised_millions INT;


SELECT *
FROM world_layoffs.layoffs_staging2
WHERE  percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

-- after this query we find that 'Britishvolt' has got 2.4B funding which is highest in our data






-- SOMEWHAT TOUGHER AND MOSTLY USING GROUP BY--------------------------------------------------------------------------------------------------

-- Companies with the biggest single Layoff

update world_layoffs.layoffs_staging
set total_laid_off = null 
where total_laid_off='NULL';


select count(total_laid_off) from 
world_layoffs.layoffs_staging
where total_laid_off='NUll';

SELECT company, total_laid_off
FROM world_layoffs.layoffs_staging
ORDER BY 2 DESC
LIMIT 5;

SELECT company, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
LIMIT 10;  




SELECT location, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY location
ORDER BY 2 DESC
LIMIT 10;-- SF Bay area has the highest no of lay offs. 


SELECT country, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;-- USA has the highest lay offs , followed by India. 



-- looking for the total no of lay offs each year 
SELECT YEAR(date), SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY YEAR(date)
ORDER BY 1 ASC; -- the trend here is increase in layoffs every year 



SELECT industry, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY industry
ORDER BY 2 DESC;-- consumer industry has the highest no of lay offs.alter

SELECT stage, SUM(total_laid_off)
FROM world_layoffs.layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC;




-- Earlier we looked at Companies with the most Layoffs. Now let's look at that per year. 
-- I want to look at 

WITH Company_Year AS 
(
  SELECT company, YEAR(date) AS years, SUM(total_laid_off) AS total_laid_off
  FROM layoffs_staging2
  GROUP BY company, YEAR(date)
)
, Company_Year_Rank AS (
  SELECT company, years, total_laid_off, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS ranking
  FROM Company_Year
)
SELECT company, years, total_laid_off, ranking
FROM Company_Year_Rank
WHERE ranking <= 3
AND years IS NOT NULL
ORDER BY years ASC, total_laid_off DESC;



-- Rolling Total of Layoffs Per Month
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC;

-- now use it in a CTE so we can query off of it
WITH DATE_CTE AS 
(
SELECT SUBSTRING(date,1,7) as dates, SUM(total_laid_off) AS total_laid_off
FROM layoffs_staging2
GROUP BY dates
ORDER BY dates ASC
)
SELECT dates, SUM(total_laid_off) OVER (ORDER BY dates ASC) as rolling_total_layoffs
FROM DATE_CTE
ORDER BY dates ASC;






