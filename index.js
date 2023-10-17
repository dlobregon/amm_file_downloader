const https = require('https');
const fs = require('fs');

// script used to generate urls and download files.
const es = require('date-fns/locale/es')
const eachDayOfInterval = require('date-fns/eachDayOfInterval')
const format = require('date-fns/format')
const path = require('path')


const baseUrl = 'https://www.amm.org.gt/pdfs2/programas_despacho/01_PROGRAMAS_DE_DESPACHO_DIARIO/2023/01_PROGRAMAS_DE_DESPACHO_DIARIO/'
const downloadFolder = path.join(__dirname, '/reportSourceFiles')


const generateUrls = () => {
    // generate the dates
    const dates = eachDayOfInterval({
        start: new Date(2023, 0, 1),
        end: new Date(2023, 5, 30)
    })

    // generate the urls 
    return dates.map((date) => {
        const month = format(date, 'MMMM', {locale: es}  )
        const monthNumber = format(date, 'MM')
        const pureDate = format(date, 'ddMMyyyy')
        return  `${baseUrl}${monthNumber}_${month.toUpperCase()}/WEB${pureDate}.xlsx`
    })
}


const DownloadExcelFile = (url, destPath) => {
    return new Promise((resolve, reject) => {
        https.get(url, res => {
            const filePath = fs.createWriteStream(destPath);
            res.pipe(filePath);
            resolve(true); 
        });    
    });
}

const createDonwloadRequests = (urls) => {
    const requests = [];
    for(const url of urls) {
        let urlObj = new URL(url);
        let parts = urlObj.pathname.split('/');
        let filename = parts[parts.length - 1];
        requests.push(DownloadExcelFile(url, `${downloadFolder}/${filename}`));
    }
    return requests;
}


(async () => {
    try {
        const urls = generateUrls()
        const requests = createDonwloadRequests(urls);
        await Promise.all(requests).then((value)=>{ 
            console.log('Download finished')
        });

    } catch(err) {
        console.log(err);
    }
})();
