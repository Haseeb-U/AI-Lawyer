/**
 * LLM Service
 * Handles interactions with OpenRouter API using Gemini 2.0 Flash
 */

import dotenv from 'dotenv';

dotenv.config();

const OPENROUTER_API_KEY = process.env.OPENROUTER_API_KEY;
const OPENROUTER_URL = 'https://openrouter.ai/api/v1/chat/completions';
// Try different free models if one is rate-limited
const MODELS = [
    "meta-llama/llama-3.3-70b-instruct:free",
    "google/gemini-2.0-flash-exp:free",
    "mistralai/mistral-small-3.2-24b-instruct:free",
    "mistralai/mistral-7b-instruct:free",
    "meta-llama/llama-3.1-8b-instruct:free",
    "deepseek/deepseek-chat-v3.1:free",
    "google/gemini-flash-1.5:free"
];
const MODEL = process.env.OPENROUTER_MODEL || MODELS[0];

class LLMService {
  /**
   * Generate a response using OpenRouter with fallback models
   * @param {string} userQuery - The user's question
   * @param {Array<Object>} context - Retrieved context chunks from vector DB
   * @returns {Promise<string>} - The generated response
   */
  async generateResponse(userQuery, context) {
    if (!OPENROUTER_API_KEY) {
      throw new Error('OPENROUTER_API_KEY is not set in environment variables');
    }

    // Build context string from retrieved chunks
    const contextString = this.buildContextString(context);

    // Create the prompt with context and query
    const prompt = this.createPrompt(userQuery, contextString);

    // Try models in order until one works
    for (let i = 0; i < MODELS.length; i++) {
      const model = MODELS[i];
      
      try {
        console.log(`🤖 Trying model: ${model}`);
        
        // Call OpenRouter API
        const response = await fetch(OPENROUTER_URL, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${OPENROUTER_API_KEY}`,
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({
            model: model,
            messages: [
              {
                role: 'system',
                content: 'You are an expert legal assistant specializing in Pakistani law. You provide accurate, helpful answers based on legal documents and precedents. Always cite the sources you reference.',
              },
              {
                role: 'user',
                content: prompt,
              },
            ],
          }),
        });

        const data = await response.json();

        if (!response.ok) {
          // If rate limited or error, try next model
          if (data.error?.code === 429 && i < MODELS.length - 1) {
            console.log(`⚠️ ${model} is rate-limited, trying next model...`);
            continue;
          }
          throw new Error(`OpenRouter API error: ${JSON.stringify(data)}`);
        }
        
        // Extract the generated text
        const generatedText = data.choices[0]?.message?.content || 'No response generated';
        console.log(`✅ Successfully generated response with ${model}`);
        
        return generatedText;
      } catch (error) {
        // If this is the last model, throw the error
        if (i === MODELS.length - 1) {
          console.error('Error generating LLM response:', error);
          throw new Error(`LLM generation failed: ${error.message}`);
        }
        // Otherwise, try next model
        console.log(`⚠️ Error with ${model}, trying next model...`);
      }
    }
  }

  /**
   * Build context string from retrieved chunks
   * @param {Array<Object>} context - Retrieved context chunks
   * @returns {string} - Formatted context string
   */
  buildContextString(context) {
    if (!context || context.length === 0) {
      return 'No relevant context found.';
    }

    return context
      .map((item, index) => {
        return `
[Source ${index + 1}]
Title: ${item.title || 'Unknown'}
Year: ${item.year || 'N/A'}
Court: ${item.court || 'N/A'}
Document Type: ${item.document_type || 'N/A'}
Content: ${item.chunk}
---`;
      })
      .join('\n');
  }

  /**
   * Create a prompt combining context and user query
   * @param {string} userQuery - The user's question
   * @param {string} contextString - Formatted context
   * @returns {string} - Complete prompt
   */
  createPrompt(userQuery, contextString) {
    return `You are an expert (responding to end-user) AI Legal Advisor specializing in Pakistani law, with deep knowledge across multiple jurisdictions including Federal, Punjab, Sindh, Balochistan, and KPK legislation, as well as Supreme Court judgments and case law.

═══════════════════════════════════════════════════════════════
LEGAL DATABASE CONTEXT:
═══════════════════════════════════════════════════════════════
${contextString}

═══════════════════════════════════════════════════════════════
CLIENT'S LEGAL QUERY:
═══════════════════════════════════════════════════════════════
${userQuery}

═══════════════════════════════════════════════════════════════
YOUR EXPERT ANALYSIS FRAMEWORK:
═══════════════════════════════════════════════════════════════

🎯 RESPONSE GUIDELINES:

1. **LANGUAGE MATCHING - CRITICAL**
   - DETECT the language of the user's query first
   - If query is in ENGLISH → Respond ONLY in English
   - If query is in ROMAN URDU (Urdu written in English script like "kya", "hai", "aap", "mujhe", "chahiye") → Respond ONLY in Roman Urdu
   - If query is in URDU SCRIPT (اردو) → Respond ONLY in Urdu script
   - NEVER mix languages in your response - maintain consistency throughout
   - Examples of Roman Urdu: "qanoon kya kehta hai?", "mujhe employee rights ke baare mein bataye", "yeh act kis saal mein bana tha?"

2. **COMPREHENSIVE ANALYSIS**
   - Start with a actual concise answer to the core question
   - Provide detailed legal reasoning with step-by-step explanation
   - Reference specific sections, articles, or clauses from the provided sources
   - Explain the practical implications and real-world applications

3. **STRUCTURED PRESENTATION**
   - Use clear headings and bullet points for readability
   - Break down complex legal concepts into digestible parts
   - Present information in logical flow: Issue → Rule → Application → Conclusion

4. **AUTHORITATIVE CITATIONS**
   - Always cite the specific source (Act name, year, section number)
   - Reference relevant case law with case names and years when available
   - Use format: "[Source: Act Name, Year, Section X]" or "[Case: Case Name, Year]"
   - Quote exact legal text when it strengthens your answer

5. **PRACTICAL GUIDANCE**
   - Explain how the law applies to the specific situation
   - Mention any exceptions, limitations, or special circumstances
   - Provide actionable insights where appropriate
   - Highlight important legal procedures or requirements

6. **TRANSPARENCY & LIMITATIONS**
   - If the provided context is insufficient, clearly state what additional information would be needed
   - Acknowledge any ambiguities in the law or interpretation
   - Recommend consulting a qualified lawyer for complex cases or final legal decisions
   - Never fabricate information not present in the context

7. **ENHANCED READABILITY**
   - Use emojis sparingly for section markers (⚖️ 📋 ⚠️ 💡) to improve visual organization
   - Employ bold and formatting for key terms and important points
   - Keep paragraphs concise and focused

═══════════════════════════════════════════════════════════════
YOUR EXPERT LEGAL RESPONSE:
═══════════════════════════════════════════════════════════════`;
  }
  
//   createPrompt(userQuery, contextString) {
//     return `You are a bilingual (English and Urdu) AI legal assistant. Based on the following legal documents and context, please answer the user's question accurately and professionally.

// CONTEXT FROM LEGAL DOCUMENTS:
// ${contextString}

// USER QUESTION:
// ${userQuery}

// INSTRUCTIONS:
// - Provide a clear, accurate answer based on the context provided
// - Cite specific sources when making legal references
// - If the context doesn't contain enough information, acknowledge this
// - Use professional legal language but keep it understandable
// - If applicable, mention relevant sections, acts, or case names

// ANSWER:`;
//   }
}

export default new LLMService();
