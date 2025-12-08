#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/json.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <cctype>
#include <set>
#include "crow.h"

struct Doc {
    std::string id;
    std::string url;
    std::vector<std::string> lemmas;
};

std::vector<Doc> corpus;


std::string strip_html_tags(const std::string &html) {
    std::string result;
    bool in_tag = false;

    for (char c: html) {
        if (c == '<') in_tag = true;
        else if (c == '>') in_tag = false;
        else if (!in_tag) result += c;
    }

    std::string clean;
    bool last_space = false;
    for (char c: result) {
        if (isspace(static_cast<unsigned char>(c))) {
            if (!last_space) {
                clean += ' ';
                last_space = true;
            }
        } else {
            clean += c;
            last_space = false;
        }
    }

    return clean;
}


std::string to_lower(const std::string &s) {
    std::string out;
    for (unsigned char c: s) out += std::tolower(c);
    return out;
}


std::vector<std::string> tokenize(const std::string &text) {
    std::vector<std::string> tokens;
    std::string cleaned;

    for (unsigned char c: text) {
        if (std::isalnum(c)) cleaned += c;
        else cleaned += ' ';
    }

    std::istringstream iss(to_lower(cleaned));
    std::string word;
    while (iss >> word) tokens.push_back(word);

    return tokens;
}

std::string simple_lemmatize(const std::string &word) {
    std::string res = word;
    if (res.length() > 4 && res.substr(res.length() - 3) == "ing") res = res.substr(0, res.length() - 3);
    else if (res.length() > 3 && res.substr(res.length() - 2) == "ed") res = res.substr(0, res.length() - 2);
    if (res.length() > 3) {
        if (res.substr(res.length() - 2) == "es") res = res.substr(0, res.length() - 2);
        else if (res.back() == 's') res = res.substr(0, res.length() - 1);
    }
    return res;
}

std::vector<std::string> lemmatize_tokens(const std::vector<std::string> &tokens) {
    std::vector<std::string> lemmas;
    for (const auto &t: tokens) lemmas.push_back(simple_lemmatize(t));
    return lemmas;
}

bool doc_matches_query(const std::vector<std::string> &doc_lemmas,
                       const std::vector<std::string> &and_terms,
                       const std::vector<std::string> &or_terms,
                       const std::vector<std::string> &not_terms) {
    std::set<std::string> lemma_set(doc_lemmas.begin(), doc_lemmas.end());

    for (const auto &t: and_terms) {
        if (lemma_set.find(t) == lemma_set.end()) return false;
    }

    if (!or_terms.empty()) {
        bool any = false;
        for (const auto &t: or_terms) {
            if (lemma_set.find(t) != lemma_set.end()) {
                any = true;
                break;
            }
        }
        if (!any) return false;
    }

    for (const auto &t: not_terms) {
        if (lemma_set.find(t) != lemma_set.end()) return false;
    }

    return true;
}

void parse_query(const std::string &query,
                 std::vector<std::string> &and_terms,
                 std::vector<std::string> &or_terms,
                 std::vector<std::string> &not_terms) {
    std::istringstream iss(query);
    std::string token;
    std::string last_op = "AND";

    while (iss >> token) {
        if (token == "AND" || token == "OR" || token == "NOT") {
            last_op = token;
        } else {
            std::string lemma = simple_lemmatize(to_lower(token));
            if (last_op == "AND") and_terms.push_back(lemma);
            else if (last_op == "OR") or_terms.push_back(lemma);
            else if (last_op == "NOT") not_terms.push_back(lemma);
        }
    }
}


int main() {
    try {
        mongocxx::instance inst{};
        mongocxx::uri uri("mongodb://root:example@localhost:27017/?authSource=admin");
        mongocxx::client conn(uri);

        auto db = conn["crawler_db"];
        auto gutenberg = db["gutenberg_docs"];
        auto standardebooks = db["standardebooks_docs"];

        std::cout << "Loading corpus from MongoDB..." << std::endl;

        // Gutenberg
        for (auto&& doc : gutenberg.find({})) {
            if (!doc["raw_html"] || !doc["url"]) continue;
            auto raw_html_view = doc["raw_html"].get_string().value;
            std::string raw_html{raw_html_view.data(), raw_html_view.size()};
            std::string text = strip_html_tags(raw_html);
            auto tokens = tokenize(text);
            auto lemmas = lemmatize_tokens(tokens);
            std::string url = doc["url"].get_string().value.data();
            std::string id = doc["_id"].get_oid().value.to_string();
            corpus.push_back({id, url, lemmas});
        }

        // Standard Ebooks
        for (auto&& doc : standardebooks.find({})) {
            if (!doc["raw_html"] || !doc["url"]) continue;
            auto raw_html_view = doc["raw_html"].get_string().value;
            std::string raw_html{raw_html_view.data(), raw_html_view.size()};
            std::string text = strip_html_tags(raw_html);
            auto tokens = tokenize(text);
            auto lemmas = lemmatize_tokens(tokens);
            std::string url = doc["url"].get_string().value.data();
            std::string id = doc["_id"].get_oid().value.to_string();
            corpus.push_back({id, url, lemmas});
        }

        std::cout << "Corpus loaded: " << corpus.size() << " documents.\n";

        crow::SimpleApp app;

        CROW_ROUTE(app, "/")([]() {
            return R"(
                <html>
                <head><title>Boolean Search</title></head>
                <body>
                    <h1>Boolean Search</h1>
                    <form action="/search" method="get">
                        <input type="text" name="query" size="50">
                        <input type="submit" value="Search">
                    </form>
                </body>
                </html>
            )";
        });

        CROW_ROUTE(app, "/search")([](const crow::request &req) {
            auto query_param = req.url_params.get("query");
            if (!query_param) return crow::response("Empty query");

            std::string query = query_param;
            std::vector<std::string> and_terms, or_terms, not_terms;
            parse_query(query, and_terms, or_terms, not_terms);

            std::vector<std::string> matched_urls;
            for (const auto &doc: corpus)
                if (doc_matches_query(doc.lemmas, and_terms, or_terms, not_terms))
                    matched_urls.push_back(doc.url);

            std::ostringstream os;
            os << "<html><body>";
            os << "<h2>Results for query: " << query << "</h2>";
            os << "<ul>";
            if (matched_urls.empty()) {
                os << "<p><b>nothinfg was founded by query((((</b></p>";
            } else {
                os << "<ul>";
                for (const auto &url: matched_urls)
                    os << "<li><a href=\"" << url << "\" target=\"_blank\">" << url << "</a></li>";
                os << "</ul>";
            }
            os << "</ul></body></html>";

            return crow::response(os.str());
        });

        std::cout << "Starting web server on http://localhost:18080\n";
        app.port(18080).multithreaded().run();
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
