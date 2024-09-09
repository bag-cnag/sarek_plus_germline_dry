// Define the first process
include {makePostComplete} from './lot3_functions'

/* def makePostComplete(hook_url,file){
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
} */

process to_elastic {
    input:
    val message4

    output:
      val message5
    exec:    
    
println message4
    message5 = "dd"
        makePostComplete(params.qc_endpoint,"elastic.json")
}