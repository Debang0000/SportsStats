from bs4 import BeautifulSoup
import re

def save_page_source(html_content, filename='page_source.html'):
    """
    Save the raw HTML content to a file for debugging.
    
    Args:
    html_content (str): The HTML content to save.
    filename (str): The file to save the HTML content to.
    """
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html_content)
    print(f"HTML content saved to {filename}")

def minify_html(html_content):
    """
    Minify the HTML content by removing unnecessary spaces, line breaks, and elements.
    
    Args:
    html_content (str): The HTML content to minify.
    
    Returns:
    str: Minified HTML content.
    """
    soup = BeautifulSoup(html_content, 'html.parser')

    # Remove scripts, styles, headers, footers
    for tag in soup(['script', 'style', 'header', 'footer']):
        tag.decompose()

    # Minify by removing extra spaces and line breaks
    minified_html = re.sub(r'\s+', ' ', soup.prettify())
    
    return minified_html
