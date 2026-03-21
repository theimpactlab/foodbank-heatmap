<?php
/**
 * Google Trends Data Fetcher for Food Bank Search Terms
 *
 * Fetches Google Trends data via the internal API endpoints.
 * Designed to be called via cPanel cron job on GoDaddy shared hosting.
 *
 * Usage:
 *   php /path/to/fetch_trends.php
 *   php /path/to/fetch_trends.php?force=1        (bypass debounce)
 */

error_reporting(E_ALL);
ini_set('display_errors', 0);
ini_set('log_errors', 1);

// Configuration
$CONFIG = [
    'search_terms' => ['food bank', 'food bank near me', 'food bank help'],
    'geo' => 'GB',
    'timeframes' => [
        'short' => 'now 7-d',
        'long' => 'today 3-m'
    ],
    'api_base' => 'https://trends.google.com/trends/api/',
    'max_retries' => 3,
    'retry_delay' => 2,
    'min_request_delay' => 2,
    'debounce_hours' => 3,
    'data_file' => __DIR__ . '/trends_data.json',
    'log_file' => __DIR__ . '/fetch_log.txt',
    'city_request_delay' => 3,  // Delay between city-level requests to avoid rate limiting
    'nations' => [
        'GB-ENG' => 'England',
        'GB-SCT' => 'Scotland',
        'GB-WLS' => 'Wales',
        'GB-NIR' => 'Northern Ireland'
    ]
];

// City coordinate lookup table
$CITY_COORDS = [
    // England
    'Pudsey' => ['lat' => 53.797, 'lng' => -1.662, 'nation' => 'England'],
    'Desborough' => ['lat' => 52.441, 'lng' => -0.818, 'nation' => 'England'],
    'Dinnington' => ['lat' => 53.368, 'lng' => -1.209, 'nation' => 'England'],
    'Middleton' => ['lat' => 53.555, 'lng' => -2.187, 'nation' => 'England'],
    'Wick' => ['lat' => 50.609, 'lng' => -2.995, 'nation' => 'England'],
    'Westoning' => ['lat' => 52.009, 'lng' => -0.486, 'nation' => 'England'],
    'Dudley' => ['lat' => 52.512, 'lng' => -2.087, 'nation' => 'England'],
    'Hastings' => ['lat' => 50.854, 'lng' => 0.573, 'nation' => 'England'],
    'Staindrop' => ['lat' => 54.583, 'lng' => -1.800, 'nation' => 'England'],
    'Worcester' => ['lat' => 52.192, 'lng' => -2.220, 'nation' => 'England'],
    'Shrewsbury' => ['lat' => 52.708, 'lng' => -2.754, 'nation' => 'England'],
    'Bushey' => ['lat' => 51.642, 'lng' => -0.360, 'nation' => 'England'],
    'Oldham' => ['lat' => 53.541, 'lng' => -2.117, 'nation' => 'England'],
    'Aldershot' => ['lat' => 51.249, 'lng' => -0.763, 'nation' => 'England'],
    'Cleobury Mortimer' => ['lat' => 52.378, 'lng' => -2.477, 'nation' => 'England'],
    'Canterbury' => ['lat' => 51.280, 'lng' => 1.080, 'nation' => 'England'],
    'Wythenshawe' => ['lat' => 53.389, 'lng' => -2.263, 'nation' => 'England'],
    'Williton' => ['lat' => 51.159, 'lng' => -3.344, 'nation' => 'England'],
    'Luton' => ['lat' => 51.879, 'lng' => -0.417, 'nation' => 'England'],
    // Scotland
    'Glasgow' => ['lat' => 55.864, 'lng' => -4.252, 'nation' => 'Scotland'],
    'Edinburgh' => ['lat' => 55.953, 'lng' => -3.189, 'nation' => 'Scotland'],
    'Aberdeen' => ['lat' => 57.150, 'lng' => -2.094, 'nation' => 'Scotland'],
    'Dundee' => ['lat' => 56.462, 'lng' => -2.970, 'nation' => 'Scotland'],
    // Wales
    'Cardiff' => ['lat' => 51.481, 'lng' => -3.179, 'nation' => 'Wales'],
    'Swansea' => ['lat' => 51.622, 'lng' => -3.943, 'nation' => 'Wales'],
    'Newport' => ['lat' => 51.589, 'lng' => -2.998, 'nation' => 'Wales'],
    // Northern Ireland
    'Belfast' => ['lat' => 54.597, 'lng' => -5.930, 'nation' => 'Northern Ireland'],
    'Derry' => ['lat' => 54.997, 'lng' => -7.319, 'nation' => 'Northern Ireland'],
    'Newry' => ['lat' => 54.176, 'lng' => -6.337, 'nation' => 'Northern Ireland']
];

// Logging helper
function log_message($message, $level = 'INFO') {
    global $CONFIG;
    $timestamp = date('Y-m-d H:i:s');
    $log_entry = "[$timestamp] [$level] $message\n";

    // Log to file
    error_log($log_entry, 3, $CONFIG['log_file']);

    // Also output to stdout if running from CLI
    if (php_sapi_name() === 'cli') {
        echo $log_entry;
    }
}

/**
 * Check if cached data is recent enough (debounce logic)
 */
function should_skip_debounce() {
    global $CONFIG;

    // Allow bypass with force parameter
    if (isset($_GET['force']) && $_GET['force'] == 1) {
        log_message('Debounce bypassed via force parameter');
        return false;
    }

    if (!file_exists($CONFIG['data_file'])) {
        return true; // No data, don't skip
    }

    $file_age_seconds = time() - filemtime($CONFIG['data_file']);
    $debounce_seconds = $CONFIG['debounce_hours'] * 3600;

    if ($file_age_seconds < $debounce_seconds) {
        $age_hours = round($file_age_seconds / 3600, 1);
        log_message("Data is $age_hours hours old, skipping fetch (debounce: {$CONFIG['debounce_hours']}h)");
        return true;
    }

    return false;
}

/**
 * Make HTTP request with retry logic and backoff
 */
function http_get($url, $headers = []) {
    global $CONFIG;

    $default_headers = [
        'User-Agent' => 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept-Language' => 'en-GB,en;q=0.9',
        'Accept' => '*/*',
        'Referer' => 'https://trends.google.com/'
    ];

    $headers = array_merge($default_headers, $headers);
    $header_array = [];
    foreach ($headers as $key => $value) {
        $header_array[] = "$key: $value";
    }

    $attempt = 0;

    while ($attempt < $CONFIG['max_retries']) {
        $attempt++;

        $ch = curl_init();
        curl_setopt_array($ch, [
            CURLOPT_URL => $url,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_TIMEOUT => 30,
            CURLOPT_CONNECTTIMEOUT => 10,
            CURLOPT_HTTPHEADER => $header_array,
            CURLOPT_FOLLOWLOCATION => true,
            CURLOPT_SSL_VERIFYPEER => true,
            CURLOPT_SSL_VERIFYHOST => 2,
            CURLOPT_COOKIEFILE => '',
            CURLOPT_COOKIEJAR => ''
        ]);

        $response = curl_exec($ch);
        $http_code = curl_getinfo($ch, CURLINFO_HTTP_CODE);
        $curl_error = curl_error($ch);
        curl_close($ch);

        if ($response !== false && $http_code >= 200 && $http_code < 300) {
            log_message("HTTP GET successful (attempt $attempt): $url");
            return $response;
        }

        log_message("HTTP GET failed (attempt $attempt/$CONFIG[max_retries]): HTTP $http_code - $curl_error", 'WARN');

        if ($attempt < $CONFIG['max_retries']) {
            $backoff = pow(2, $attempt - 1) * $CONFIG['retry_delay'];
            log_message("Retrying in {$backoff}s...", 'WARN');
            sleep($backoff);
        }
    }

    return null;
}

/**
 * Strip Google's JSON prefix: )]}'
 */
function strip_json_prefix($response) {
    if (strpos($response, ")]}") === 0) {
        return substr($response, 5);
    }
    return $response;
}

/**
 * Fetch widget tokens for search terms
 */
function fetch_widget_tokens() {
    global $CONFIG;
    $tokens = [];

    foreach ($CONFIG['search_terms'] as $term) {
        log_message("Fetching widget token for: '$term'");

        $params = http_build_query([
            'hl' => 'en-GB',
            'tz' => 0,
            'req' => json_encode([
                'comparisonItem' => [
                    [
                        'keyword' => $term,
                        'geo' => $CONFIG['geo'],
                        'time' => $CONFIG['timeframes']['long']
                    ]
                ]
            ]),
            'property' => ''
        ]);

        $url = $CONFIG['api_base'] . 'explore?' . $params;
        $response = http_get($url);

        if ($response === null) {
            log_message("Failed to fetch widget token for '$term'", 'ERROR');
            return null;
        }

        $response = strip_json_prefix($response);
        $data = json_decode($response, true);

        if (!isset($data['widgets'][0]['id'])) {
            log_message("Invalid widget response for '$term'", 'ERROR');
            return null;
        }

        $tokens[$term] = $data['widgets'][0]['id'];
        log_message("Widget token obtained for '$term': {$tokens[$term]}");

        sleep($CONFIG['min_request_delay']);
    }

    return $tokens;
}

/**
 * Fetch interest by region data
 */
function fetch_interest_by_region($widget_token) {
    global $CONFIG;

    $params = http_build_query([
        'hl' => 'en-GB',
        'tz' => 0,
        'req' => json_encode([
            'restriction' => [
                'geo' => $CONFIG['geo'],
                'time' => $CONFIG['timeframes']['short']
            ],
            'resolution' => 'REGION'
        ]),
        'token' => $widget_token
    ]);

    $url = $CONFIG['api_base'] . 'regional_interest_by_region?' . $params;
    $response = http_get($url);

    if ($response === null) {
        return null;
    }

    $response = strip_json_prefix($response);
    $data = json_decode($response, true);

    return $data['default']['geoMapData'] ?? null;
}

/**
 * Fetch interest over time (national trend)
 */
function fetch_interest_over_time($widget_token) {
    global $CONFIG;

    $params = http_build_query([
        'hl' => 'en-GB',
        'tz' => 0,
        'req' => json_encode([
            'restriction' => [
                'geo' => $CONFIG['geo'],
                'time' => $CONFIG['timeframes']['long']
            ]
        ]),
        'token' => $widget_token
    ]);

    $url = $CONFIG['api_base'] . 'interest_over_time?' . $params;
    $response = http_get($url);

    if ($response === null) {
        return null;
    }

    $response = strip_json_prefix($response);
    $data = json_decode($response, true);

    return $data['default']['timelineData'] ?? null;
}

/**
 * Fetch city-level interest data for a specific nation
 */
function fetch_interest_by_city($widget_token, $nation_code) {
    global $CONFIG;

    $params = http_build_query([
        'hl' => 'en-GB',
        'tz' => 0,
        'req' => json_encode([
            'restriction' => [
                'geo' => $nation_code,
                'time' => $CONFIG['timeframes']['short']
            ],
            'resolution' => 'CITY'
        ]),
        'token' => $widget_token
    ]);

    $url = $CONFIG['api_base'] . 'regional_interest_by_region?' . $params;
    $response = http_get($url);

    if ($response === null) {
        return null;
    }

    $response = strip_json_prefix($response);
    $data = json_decode($response, true);

    return $data['default']['geoMapData'] ?? null;
}

/**
 * Parse regional data and compute weighted averages
 */
function parse_regional_data($regions_by_term) {
    $regions_map = [];

    // Aggregate data by region name
    foreach ($regions_by_term as $term => $region_data) {
        if (!is_array($region_data)) {
            continue;
        }

        foreach ($region_data as $item) {
            $region_name = $item['geoName'] ?? '';
            if (!$region_name) {
                continue;
            }

            if (!isset($regions_map[$region_name])) {
                $regions_map[$region_name] = [
                    'scores_7d' => [],
                    'scores_90d' => [],
                    'terms' => []
                ];
            }

            $score = $item['value'][0] ?? 0;
            $regions_map[$region_name]['scores_7d'][] = $score;
            $regions_map[$region_name]['terms'][$term] = $score;
        }
    }

    // Compute weighted averages and format output
    $regions_output = [];

    foreach ($regions_map as $region_name => $data) {
        if (empty($data['scores_7d'])) {
            continue;
        }

        $score_7d = round(array_sum($data['scores_7d']) / count($data['scores_7d']));

        $regions_output[] = [
            'name' => $region_name,
            'score_7d' => $score_7d,
            'score_90d' => 0, // Will be populated if 90d data available
            'terms' => $data['terms']
        ];
    }

    // Sort by score descending
    usort($regions_output, function($a, $b) {
        return $b['score_7d'] - $a['score_7d'];
    });

    return $regions_output;
}

/**
 * Parse interest over time into daily records
 */
function parse_interest_over_time($timeline_data) {
    if (!is_array($timeline_data)) {
        return [];
    }

    $national_trend = [];

    foreach ($timeline_data as $item) {
        $timestamp = $item['time'] ?? 0;
        $score = $item['value'][0] ?? 0;

        if ($timestamp > 0) {
            $date = date('Y-m-d', (int)$timestamp);
            $national_trend[] = [
                'date' => $date,
                'score' => $score
            ];
        }
    }

    return $national_trend;
}

/**
 * Parse city-level data with geocoding from lookup table
 */
function parse_city_data($cities_by_nation) {
    global $CITY_COORDS;

    $cities_output = [];

    foreach ($cities_by_nation as $nation_code => $city_data) {
        if (!is_array($city_data)) {
            continue;
        }

        foreach ($city_data as $item) {
            $city_name = $item['geoName'] ?? '';
            if (!$city_name) {
                continue;
            }

            $score = $item['value'][0] ?? 0;

            // Look up coordinates in the static lookup table
            if (isset($CITY_COORDS[$city_name])) {
                $coords = $CITY_COORDS[$city_name];
                $cities_output[] = [
                    'name' => $city_name,
                    'score' => $score,
                    'nation' => $coords['nation'],
                    'lat' => $coords['lat'],
                    'lng' => $coords['lng']
                ];
            } else {
                log_message("City '$city_name' from $nation_code not found in coordinate lookup, skipping", 'WARN');
            }
        }
    }

    // Sort by score descending
    usort($cities_output, function($a, $b) {
        return $b['score'] - $a['score'];
    });

    return $cities_output;
}

/**
 * Load existing cached data (fallback on error)
 */
function load_existing_data() {
    global $CONFIG;

    if (!file_exists($CONFIG['data_file'])) {
        return null;
    }

    $json = @file_get_contents($CONFIG['data_file']);
    if ($json === false) {
        return null;
    }

    return json_decode($json, true);
}

/**
 * Save data to file (atomic write)
 */
function save_data($data) {
    global $CONFIG;

    $json = json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . "\n";

    // Use temporary file for atomic write
    $temp_file = $CONFIG['data_file'] . '.tmp';
    if (file_put_contents($temp_file, $json, LOCK_EX) === false) {
        log_message("Failed to write temporary file", 'ERROR');
        return false;
    }

    if (!rename($temp_file, $CONFIG['data_file'])) {
        log_message("Failed to rename temporary file", 'ERROR');
        @unlink($temp_file);
        return false;
    }

    log_message("Data saved successfully to {$CONFIG['data_file']}");
    return true;
}

/**
 * Main execution
 */
function main() {
    global $CONFIG;

    log_message("=== Food Bank Trends Fetch Started ===");

    // Check debounce
    if (should_skip_debounce()) {
        log_message("Fetch skipped (debounce active)");
        return;
    }

    // Fetch widget tokens
    $tokens = fetch_widget_tokens();
    if ($tokens === null) {
        log_message("Failed to fetch widget tokens, keeping existing data", 'ERROR');
        return;
    }

    // Fetch regional interest data for each term
    $regions_by_term = [];
    foreach ($tokens as $term => $token) {
        log_message("Fetching regional interest for '$term'");
        $regions = fetch_interest_by_region($token);

        if ($regions === null) {
            log_message("Failed to fetch regional interest for '$term'", 'WARN');
            continue;
        }

        $regions_by_term[$term] = $regions;
        sleep($CONFIG['min_request_delay']);
    }

    if (empty($regions_by_term)) {
        log_message("No regional data fetched, keeping existing data", 'ERROR');
        return;
    }

    // Fetch national trend using first term's token
    $first_token = reset($tokens);
    log_message("Fetching national trend data");
    $timeline = fetch_interest_over_time($first_token);

    if ($timeline === null) {
        log_message("Failed to fetch national trend, keeping existing data", 'WARN');
        $timeline = [];
    }

    // Fetch city-level data for each nation
    $cities_by_nation = [];
    log_message("Fetching city-level data for each nation");
    foreach ($CONFIG['nations'] as $nation_code => $nation_name) {
        log_message("Fetching city data for $nation_name ($nation_code)");
        $city_data = fetch_interest_by_city($first_token, $nation_code);

        if ($city_data === null) {
            log_message("Failed to fetch city data for $nation_name", 'WARN');
        } else {
            $cities_by_nation[$nation_code] = $city_data;
        }

        // Delay between requests to avoid rate limiting
        sleep($CONFIG['city_request_delay']);
    }

    // Parse and aggregate data
    $regions = parse_regional_data($regions_by_term);
    $national_trend = parse_interest_over_time($timeline);
    $cities = parse_city_data($cities_by_nation);

    // Build output structure
    $output = [
        'last_updated' => date('c'),
        'timeframe_short' => $CONFIG['timeframes']['short'],
        'timeframe_long' => $CONFIG['timeframes']['long'],
        'regions' => $regions,
        'national_trend' => $national_trend,
        'cities' => $cities
    ];

    // Save to file
    if (save_data($output)) {
        log_message("=== Food Bank Trends Fetch Completed Successfully ===");
    } else {
        log_message("=== Food Bank Trends Fetch Failed to Save ===", 'ERROR');
    }
}

// Run main
main();
?>
