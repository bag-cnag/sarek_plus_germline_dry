def create_file(file_name){
    def outputFile = new File(file_name)
    def myString = "This is the content of the file"
    outputFile.getParentFile().mkdirs()

      outputFile.write(myString)


}
def makePostComplete(hook_url,file){
    def engine       = new groovy.text.GStringTemplateEngine()
    def hf            = new File("${projectDir}/bin/${file}")
    def json_template = engine.createTemplate(hf).make()
    def json_message  = json_template.toString()
    //println(json_message)
    // POST
    def post = new URL(hook_url).openConnection();
    post.setRequestMethod("POST")
    post.setDoOutput(true)
    post.setRequestProperty("Content-Type", "application/json")
    post.getOutputStream().write(json_message.getBytes("UTF-8"));
    def postRC = post.getResponseCode();
    if (! postRC.equals(201)) {
        log.warn(post.getErrorStream().getText());
    }
} 
