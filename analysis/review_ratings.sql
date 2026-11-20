-- One row per scraped review. UNION ALL preserves the source table grain.
-- Hotels and restaurants store the two-digit TripAdvisor bubble class suffix.
-- Attractions store the numeric rating from the accessible label.
WITH review_ratings AS (
    SELECT 'Hotel' AS category, rating, 'bubble' AS encoding FROM HotelReview
    UNION ALL
    SELECT 'Restaurant', rating, 'bubble' FROM RestaurantReview
    UNION ALL
    SELECT 'Attraction', rating, 'stars' FROM AttractionReview
),
normalized AS (
    SELECT category,
           CASE
               WHEN encoding = 'bubble' AND TRIM(rating) IN ('10','20','30','40','50')
                   THEN CAST(rating AS DECIMAL(4,1)) / 10.0
               WHEN encoding = 'stars' AND TRIM(rating) IN
                   ('1','1.0','1.5','2','2.0','2.5','3','3.0','3.5','4','4.0','4.5','5','5.0')
                   THEN CAST(rating AS DECIMAL(4,1))
           END AS stars
    FROM review_ratings
)
SELECT category, COUNT(*) AS reviews, COUNT(stars) AS scored,
       COUNT(*) - COUNT(stars) AS unscored, ROUND(AVG(stars), 2) AS mean_stars
FROM normalized
GROUP BY category
ORDER BY category;
