const Apify = require('apify');
const { parse } = require('url');
const { writeRecords } = require('csv-writer').createObjectCsvWriter;
const cheerio = require('cheerio');

Apify.main(async () => {
    // Get input data (list of Yelp URLs)
    const input = await Apify.getInput();
    const yelpUrls = input.urls || [];
    
    if (!yelpUrls.length) {
        throw new Error('Please provide at least one Yelp URL in the input');
    }

    // Prepare dataset for results
    const dataset = await Apify.openDataset();
    
    // Create CSV writer
    const csvWriter = writeRecords({
        path: 'yelp_data.csv',
        header: [
            {id: 'encid', title: 'Business ID'},
            {id: 'name', title: 'Business Name'},
            {id: 'categories', title: 'Categories'},
            {id: 'priceRange', title: 'Price Range'},
            {id: 'phone', title: 'Phone Number'},
            {id: 'address', title: 'Address'},
            {id: 'city', title: 'City'},
            {id: 'state', title: 'State'},
            {id: 'zipCode', title: 'ZIP Code'},
            {id: 'country', title: 'Country'},
            {id: 'rating', title: 'Rating'},
            {id: 'reviewCount', title: 'Review Count'},
            {id: 'url', title: 'URL'},
        ]
    });

    // Launch Puppeteer
    const browser = await Apify.launchPuppeteer();
    
    for (const url of yelpUrls) {
        try {
            console.log(`Processing: ${url}`);
            
            const page = await browser.newPage();
            
            // Set headers to mimic a browser
            await page.setExtraHTTPHeaders({
                'accept-language': 'en-US,en;q=0.9',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            });
            
            // Navigate to the page with a delay
            await page.goto(url, {
                waitUntil: 'networkidle2',
                timeout: 60000
            });
            
            // Wait for critical elements to load
            await page.waitForSelector('[class*="businessTitle"]', { timeout: 30000 });
            
            // Extract business ID from URL
            const businessId = parse(url).pathname.split('/').pop();
            
            // Get page content
            const html = await page.content();
            const $ = cheerio.load(html);
            
            // Extract business data
            const businessData = {
                encid: businessId,
                name: $('h1').first().text().trim(),
                categories: $('[class*="category-str-list"] a').map((i, el) => $(el).text().trim()).get().join('; '),
                priceRange: $('[class*="price-range"]').first().text().trim(),
                phone: $('[class*="phone"]').first().text().trim(),
                address: $('[class*="address"]').first().text().trim().replace(/\n/g, ', '),
                city: $('[class*="address-city"]').first().text().trim(),
                state: $('[class*="address-state"]').first().text().trim(),
                zipCode: $('[class*="address-postal"]').first().text().trim(),
                country: $('[class*="address-country"]').first().text().trim(),
                rating: parseFloat($('[class*="rating"] meta[itemprop="ratingValue"]').attr('content') || '0'),
                reviewCount: parseInt($('[class*="rating"] meta[itemprop="reviewCount"]').attr('content') || '0'),
                url: url
            };
            
            // Save to dataset
            await dataset.pushData(businessData);
            
            // Write to CSV
            await csvWriter.writeRecords([businessData]);
            
            console.log(`Successfully scraped: ${businessData.name}`);
            
            // Close the page to free memory
            await page.close();
            
            // Add delay between requests
            await Apify.utils.sleep(2000);
            
        } catch (error) {
            console.error(`Error processing ${url}:`, error.message);
        }
    }
    
    // Close browser
    await browser.close();
    
    console.log('Scraping completed!');
});
