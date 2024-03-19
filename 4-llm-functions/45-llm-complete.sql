SELECT SNOWFLAKE.CORTEX.COMPLETE(
   'mistral-large', 'What are large language models?');

SELECT SNOWFLAKE.CORTEX.COMPLETE(
   'mistral-large', CONCAT('Critique this review in bullet points: <review>', content, '</review>'))
FROM reviews LIMIT 10;

SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-7b-chat',
   [{
      'role': 'user',
      'content': 'how does a snowflake get its unique pattern?'
   }], {
      'temperature': 0.7,
      'max_tokens': 10
   });

SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat',
   [{ 'role': 'system', 'content': 'You are a helpful AI assistant. Analyze the movie review text and determine the overall sentiment. Answer with just \"Positive\", \"Negative\", or \"Neutral\"' },
   { 'role': 'user', 'content': 'this was really good' }], {}) as response;
