<?php
/**
 * Food Bank Trends Data API Endpoint
 *
 * Serves cached Google Trends data as JSON with proper headers.
 * Designed for use on GoDaddy shared hosting.
 */

// Security headers and content type
header('Content-Type: application/json; charset=utf-8');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: GET, OPTIONS');
header('Access-Control-Allow-Headers: Content-Type');
header('Cache-Control: public, max-age=1800'); // 30 minutes
header('X-Content-Type-Options: nosniff');

// Handle OPTIONS request for CORS preflight
if ($_SERVER['REQUEST_METHOD'] === 'OPTIONS') {
    http_response_code(204);
    exit;
}

// Only allow GET requests
if ($_SERVER['REQUEST_METHOD'] !== 'GET') {
    http_response_code(405);
    echo json_encode([
        'error' => 'Method not allowed',
        'timestamp' => date('c')
    ]);
    exit;
}

/**
 * Load and return trends data
 */
function load_trends_data() {
    $data_file = __DIR__ . '/trends_data.json';

    if (!file_exists($data_file)) {
        return [
            'error' => 'Trends data not available',
            'message' => 'Data has not been fetched yet. Run fetch_trends.php to initialize.',
            'timestamp' => date('c')
        ];
    }

    $json_content = @file_get_contents($data_file);

    if ($json_content === false) {
        return [
            'error' => 'Failed to read trends data',
            'timestamp' => date('c')
        ];
    }

    $data = json_decode($json_content, true);

    if ($data === null) {
        return [
            'error' => 'Corrupted trends data',
            'message' => 'The cached JSON file is malformed.',
            'timestamp' => date('c')
        ];
    }

    return $data;
}

$data = load_trends_data();
echo json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES);
?>
