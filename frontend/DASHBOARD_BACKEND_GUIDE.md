# Backend requirements for Dashboard Integation

This file was automatically generated to assist the Backend Developer in fulfilling the API calls implemented in the Frontend Dashboard. 
The Frontend expects the following APIs and DB schemas to operate with 100% Real User Data.

## 1. Database Schema Changes Required
Currently, the database only tracks Chat History properly. To feed real data into the Dashboard, we need to track user actions across all modules.

### A. `user_activity_logs` Table (Recommended)
Create a new table/collection to log all user actions globally.
*   **Columns/Fields**:
    *   `id` (PK)
    *   `user_id` (FK)
    *   `module` (enum: 'Contract Analysis', 'AI Consultant', 'Judgments Library', 'Legal Search')
    *   `action` (e.g., 'analyzed_contract', 'asked_question', 'viewed_judgment')
    *   `title` (string, e.g., "Lease_Agreement_2024.pdf" or "Searched: Section 144")
    *   `created_at` (timestamp)

### B. Module-Specific Tracking
*   **Contracts**: Ensure that when `POST /contracts/analyze` is called, a record of the uploaded file and its metadata is saved to a `user_contracts` table tied to the `user_id`.
*   **Judgments**: When `GET /judgments/:title/summary` is called, insert a log into `user_activity_logs`.

## 2. Required New API Endpoints

### Endpoint 1: Dashboard Stats
*   **Route**: `GET /api/dashboard/stats`
*   **Purpose**: Returns the top 4 KPI metrics for the current logged-in user.
*   **Headers**: `Authorization: Bearer <TOKEN>`
*   **Expected JSON Response**:
```json
{
  "aiConsultations": 24,            // Count of user's chats from `chats` table
  "contractsAnalyzed": 8,           // Count of user's contracts from `user_contracts` table
  "judgmentsViewed": 142,           // Count of unique judgments viewed from `user_activity_logs`
  "hoursSaved": "32h"               // Estimated formula: (contractsAnalyzed * 2.5) + (judgmentsViewed * 0.5) + "h"
}
```

### Endpoint 2: Recent Activity Timeline
*   **Route**: `GET /api/dashboard/activity`
*   **Purpose**: Returns the 5 most recent actions performed by the user to populate the timeline.
*   **Headers**: `Authorization: Bearer <TOKEN>`
*   **Expected JSON Response**:
```json
[
  {
    "id": "item1",
    "title": "Reviewed Lease Agreement.pdf",
    "time": "Today at 10:24 AM",
    "module": "Contract Analysis",
    "color": "bg-emerald-500",
    "iconType": "file" // choices: 'file', 'message', 'gavel', 'search'
  },
  {
    "id": "item2",
    "title": "Chat query regarding 'Tenant Eviction Rules'",
    "time": "Yesterday at 4:20 PM",
    "module": "AI Consultant",
    "color": "bg-blue-500",
    "iconType": "message"
  }
]
```
*(Note: Ensure the backend formats the `time` as a readable string or passing the standard ISO timestamp so the frontend can parse it).*

### Endpoint 3: System Status (Optional but Recommended)
*   **Route**: `GET /api/dashboard/system-status`
*   **Purpose**: Gives real-time data on backend indexing or user usage limits.
*   **Expected JSON Response**:
```json
{
  "databaseIndexing": 100,           // Number 0-100 indicating judgment database health
  "usageQuotaPercentage": 45,        // Number 0-100 indicating how much of user's monthly AI limit is consumed
  "statusMessage": "Your legal assistant is fully operational and synchronized with the latest federal amendments."
}
```

## Summary
Once these 3 routes are created on your backend (Node.js/Express or Python), the frontend React `Dashboard.jsx` and `dashboard.service.js` will automatically pick up the real information, replacing any `0s` or fallback skeletons with production database records.