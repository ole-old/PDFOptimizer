::cd F:\Couch-Node\DownloadedPdfs
cd %2
gm convert -density 144 -background white %1 +adjoin page-%%d.jpg
::%2
::"C:\Program Files\ImageMagick-6.8.9-Q16\convert.exe" -density 288 -background white -alpha remove HBLPbodygirls.pdf page.jpg
::java -jar "F:\PDF_Optimizer\pdfbox-app-1.8.5.jar" PDFToImage -outputPrefix page %1
::set rootPath=F:\PDF_Optimizer\temp    // -resize 50%%
::"C:\Program Files (x86)\Softinterface, Inc\Convert PDF To Image\ConvertPDFToImage.exe"  /S %1   /T "%2%\page.jpg"   /1 *   /4 75   /5 200   /V
::%rootPath%