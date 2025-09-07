import os
import numpy as np
import pandas as pd
import torch
import rasterio
from rasterio.transform import from_bounds
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from tqdm import tqdm
import torch.nn as nn
import torchvision.models as models
import torch.nn.functional as F
import json
import contextily as ctx
from rasterio.warp import transform_bounds

from get_rasters import (
    extract_data,
    BUFFER_DEG
)

class MapClassifier:
    def __init__(self, model_path, model_name='resnet34', num_classes=2, class_names=None):
        """
        Initialize the map classifier
        
        Args:
            model_path (str): Path to trained model weights
            model_name (str): Model architecture name
            num_classes (int): Number of classes
            class_names (list): Optional class names for visualization
        """
        self.model_path = model_path
        self.model_name = model_name
        self.num_classes = num_classes
        self.class_names = class_names or [f'Class_{i}' for i in range(num_classes)]
        self.device = 'cuda' if torch.cuda.is_available() else 'cpu'
        
        self.model = self._load_model()
        
    def _load_model(self):
        """Load the trained model"""
        
        model = models.resnet34(pretrained=True)
        model.conv1 = nn.Conv2d(in_channels=8, out_channels=64, kernel_size=7, stride=2, padding=3, bias=False)
        model.fc = nn.Linear(model.fc.in_features, out_features=2)

        model.load_state_dict(torch.load(self.model_path, map_location='cpu'))
        model.to(self.device)
        model.eval()
        print(f"Model loaded successfully on {self.device}")
        return model
    
    def create_grid(self, bbox):
        """
        Create a grid of points from a bounding box
        
        Args:
            bbox (list): [min_lon, min_lat, max_lon, max_lat]
            grid_size (tuple): Optional (rows, cols). If None, calculated based on BUFFER_DEG
            
        Returns:
            tuple: (grid_points, grid_shape, cell_size)
        """
        min_lon, min_lat, max_lon, max_lat = bbox
        
        cell_size_deg = BUFFER_DEG * 2
        
        n_cols = int(np.ceil((max_lon - min_lon) / cell_size_deg))
        n_rows = int(np.ceil((max_lat - min_lat) / cell_size_deg))
        
        print(f"Creating grid: {n_rows} x {n_cols} cells")
        
        # Create grid points
        grid_points = []
        for row in range(n_rows):
            for col in range(n_cols):
                # Calculate cell center
                lon = min_lon + (col + 0.5) * cell_size_deg
                lat = min_lat + (row + 0.5) * cell_size_deg
                
                # if lon <= max_lon and lat <= max_lat:
                grid_points.append({
                    'row': row,
                    'col': col,
                    'latitude': lat,
                    'longitude': lon,
                    'name': ''
                })
        
        return grid_points, (n_rows, n_cols), cell_size_deg
    
    def extract_features_for_grid(self, grid_points, temp_folder='data/temp_grid_data'):
        """
        Extract features for all grid points using your existing extract_data function
        
        Args:
            grid_points (list): List of grid point dictionaries
            temp_folder (str): Temporary folder for storing feature cubes
            max_workers (int): Number of parallel workers
            
        Returns:
            list: List of successfully processed file paths
        """
        os.makedirs(temp_folder, exist_ok=True)

        locations_df = pd.DataFrame(grid_points)

        print(f"Extracting features for {len(grid_points)} grid cells...")

        extract_data(locations_df, temp_folder)
        
        feature_files = []
        for idx, point in enumerate(grid_points):
            file_path = f"{temp_folder}/feature_cube_{idx}.tif"
            if os.path.exists(file_path):
                feature_files.append((file_path, point['row'], point['col']))
            else:
                print(f"Warning: Feature cube not created for grid cell {idx}")
        
        print(f"Successfully extracted features for {len(feature_files)}/{len(grid_points)} grid cells")
        return feature_files
    
    def predict_single_image(self, model, image_path, device='cuda', class_names=None):
        """
        Predict on a single image using a trained model
        
        Args:
            model (torch.nn.Module): Trained model
            image_path (str or Path): Path to the image file (.tif)
            device (str): Device to run prediction on ('cuda' or 'cpu')
            return_probabilities (bool): If True, return probabilities instead of just prediction
            class_names (list): Optional list of class names for readable output
            
        Returns:
            dict: Dictionary containing prediction results
        """
        with rasterio.open(image_path) as src:
            arr = src.read()  # shape: (C, H, W)
            
            image = arr[:-1].astype(np.float32)  # (C-1, H, W)
            mask = arr[-1].astype(np.int64)  # (H, W)
            true_label = mask[0, 0] 
            
            # Sanitize image
            image = np.nan_to_num(image, nan=0.0, posinf=0.0, neginf=0.0)
        
        image_tensor = torch.tensor(image, dtype=torch.float32).unsqueeze(0)  # (1, C, H, W)
        image_tensor = image_tensor.to(device)

        with torch.no_grad():
            outputs = model(image_tensor)

            probabilities = F.softmax(outputs, dim=1)
            
            predicted_class = torch.argmax(outputs, dim=1).item()
            confidence = probabilities[0, predicted_class].item()

        results = {
            'predicted_class': predicted_class,
            'confidence': confidence,
            'true_label': true_label,
            'correct': predicted_class == true_label,
            'image_path': str(image_path),
            'image_shape': image.shape
        }
        
        if class_names:
            results['predicted_class_name'] = class_names[predicted_class]
            results['true_class_name'] = class_names[true_label]
        
        return results

    def classify_grid(self, feature_files, grid_shape):
        """
        Classify all grid cells and create 2D prediction array
        
        Args:
            feature_files (list): List of (file_path, row, col) tuples
            grid_shape (tuple): Shape of the grid (n_rows, n_cols)
            
        Returns:
            tuple: (predictions_2d, confidences_2d, results_list)
        """
        n_rows, n_cols = grid_shape
        
        predictions_2d = np.full((n_rows, n_cols), -1, dtype=int)
        confidences_2d = np.full((n_rows, n_cols), np.nan, dtype=float)
        
        results_list = []
        
        print(f"Classifying {len(feature_files)} grid cells...")
        
        for file_path, row, col in tqdm(feature_files, desc="Classifying"):
            try:
                result = self.predict_single_image(
                    self.model, 
                    file_path
                )
                
                predictions_2d[row, col] = result['predicted_class']
                confidences_2d[row, col] = result['confidence']
                
                result['grid_row'] = row
                result['grid_col'] = col
                results_list.append(result)
                
            except Exception as e:
                print(f"Error classifying grid cell ({row}, {col}): {e}")
                break
        
        return predictions_2d, confidences_2d, results_list
    
    def visualize_results(self, predictions_2d, confidences_2d, bbox, save_path=None):
        """
        Visualize classification results
        
        Args:
            predictions_2d (np.array): 2D array of predictions
            confidences_2d (np.array): 2D array of confidences
            bbox (list): Original bounding box
            save_path (str): Optional path to save the visualization
        """
        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        
        min_lon, min_lat, max_lon, max_lat = bbox
        # Plot predictions
        im1 = axes[0].imshow(predictions_2d, cmap='viridis', origin='lower')
        axes[0].set_title('Classification Results')
        axes[0].set_xlabel('Grid Column (Longitude)')
        axes[0].set_ylabel('Grid Row (Latitude)')
        
        
        # Add colorbar with class names
        cbar1 = plt.colorbar(im1, ax=axes[0])
        if len(self.class_names) <= 10:  # Only show labels if reasonable number
            cbar1.set_ticks(range(len(self.class_names)))
            cbar1.set_ticklabels(self.class_names)
        
        # Plot confidence
        im2 = axes[1].imshow(confidences_2d, cmap='plasma', origin='lower', vmin=0, vmax=1)
        axes[1].set_title('Prediction Confidence')
        axes[1].set_xlabel('Grid Column (Longitude)')
        axes[1].set_ylabel('Grid Row (Latitude)')
        plt.colorbar(im2, ax=axes[1], label='Confidence')
        
        # Add bounding box info
        fig.suptitle(f'Map Classification Results\n'
                    f'Area: {min_lat:.4f}°-{max_lat:.4f}°N, {min_lon:.4f}°-{max_lon:.4f}°E')
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight')
            print(f"Visualization saved to {save_path}")
        
        plt.show()
    
    def save_results(self, predictions_2d, confidences_2d, bbox, grid_points, output_folder='classification_results'):
        """
        Save classification results in multiple formats
        
        Args:
            predictions_2d (np.array): 2D predictions array
            confidences_2d (np.array): 2D confidences array
            bbox (list): Bounding box
            grid_points (list): Original grid points
            output_folder (str): Output folder path
        """
        os.makedirs(output_folder, exist_ok=True)
        
        # Save as numpy arrays
        np.save(f"{output_folder}/predictions_2d.npy", predictions_2d)
        np.save(f"{output_folder}/confidences_2d.npy", confidences_2d)
        
        # Save as GeoTIFF
        n_rows, n_cols = predictions_2d.shape
        transform = from_bounds(*bbox, width=n_cols, height=n_rows)
        
        # Save predictions as GeoTIFF
        with rasterio.open(
            f"{output_folder}/predictions.tif",
            'w', driver='GTiff', height=n_rows, width=n_cols,
            count=1, dtype=predictions_2d.dtype, crs='EPSG:4326',
            transform=transform
        ) as dst:
            dst.write(predictions_2d, 1)
        
        # Save confidences as GeoTIFF
        with rasterio.open(
            f"{output_folder}/confidences.tif",
            'w', driver='GTiff', height=n_rows, width=n_cols,
            count=1, dtype=confidences_2d.dtype, crs='EPSG:4326',
            transform=transform
        ) as dst:
            dst.write(confidences_2d, 1)
        
        # Save metadata
        metadata = {
            'bbox': bbox,
            'grid_shape': predictions_2d.shape,
            'class_names': self.class_names,
            'total_cells': len(grid_points),
            'classified_cells': int(np.sum(predictions_2d >= 0)),
            'model_path': self.model_path,
            'model_name': self.model_name
        }
        
        with open(f"{output_folder}/metadata.json", 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"Results saved to {output_folder}/")
        return output_folder

    def visualize_results_with_map_overlay(self, predictions_2d, confidences_2d, bbox, save_path=None, 
                                        map_source=ctx.providers.CartoDB.Positron, alpha=0.6,
                                        figsize=(15, 10), show_confidence=True):
        """
        Visualize classification results overlaid on a real map
        
        Args:
            predictions_2d (np.array): 2D array of predictions
            confidences_2d (np.array): 2D array of confidences
            bbox (list): Original bounding box [min_lon, min_lat, max_lon, max_lat]
            save_path (str): Optional path to save the visualization
            map_source: Contextily map provider (default: CartoDB.Positron)
            alpha (float): Transparency of the overlay (0-1)
            figsize (tuple): Figure size
            show_confidence (bool): Whether to show confidence subplot
        """
        min_lon, min_lat, max_lon, max_lat = bbox
        
        # Create figure with subplots
        if show_confidence:
            fig, axes = plt.subplots(1, 2, figsize=figsize)
            ax1, ax2 = axes
        else:
            fig, ax1 = plt.subplots(1, 1, figsize=(figsize[0]//2, figsize[1]))
            axes = [ax1]
        
        # Transform bbox to Web Mercator (EPSG:3857) for contextily
        try:
            west_3857, south_3857, east_3857, north_3857 = transform_bounds(
                'EPSG:4326', 'EPSG:3857', min_lon, min_lat, max_lon, max_lat
            )
        except Exception as e:
            print(f"Warning: Could not transform coordinates: {e}")
            # Fallback to approximate Web Mercator transformation
            west_3857 = min_lon * 111320
            east_3857 = max_lon * 111320
            south_3857 = np.log(np.tan(np.radians(min_lat)/2 + np.pi/4)) * 6378137
            north_3857 = np.log(np.tan(np.radians(max_lat)/2 + np.pi/4)) * 6378137
        
        # Create extent for imshow (Web Mercator coordinates)
        extent_3857 = [west_3857, east_3857, south_3857, north_3857]
        
        # Plot 1: Predictions with map overlay
        ax1.set_xlim(west_3857, east_3857)
        ax1.set_ylim(south_3857, north_3857)
        
        # Add base map
        try:
            ctx.add_basemap(ax1, crs='EPSG:3857', source=map_source, alpha=0.8)
        except Exception as e:
            print(f"Warning: Could not load base map: {e}")
            print("Continuing without base map...")
        
        # Create custom colormap for predictions
        n_classes = len(self.class_names)
        colors = plt.cm.Set3(np.linspace(0, 1, n_classes))  # Use Set3 for distinct colors
        cmap_pred = ListedColormap(colors)
        
        # Mask invalid predictions for transparency
        predictions_masked = np.ma.masked_where(predictions_2d == -1, predictions_2d)
        
        # Overlay predictions
        im1 = ax1.imshow(predictions_masked, extent=extent_3857, origin='lower', 
                        cmap=cmap_pred, alpha=alpha, vmin=0, vmax=n_classes-1)
        
        ax1.set_title('Classification Results on Map', fontsize=14, pad=20)
        ax1.set_xlabel('Longitude', fontsize=12)
        ax1.set_ylabel('Latitude', fontsize=12)
        
        # Add colorbar with class names for predictions
        cbar1 = plt.colorbar(im1, ax=ax1, shrink=0.8, pad=0.02)
        cbar1.set_label('Predicted Class', fontsize=11)
        if n_classes <= 10:  # Only show labels if reasonable number
            cbar1.set_ticks(range(n_classes))
            cbar1.set_ticklabels(self.class_names, fontsize=9)
        
        # Plot 2: Confidence with map overlay (if requested)
        if show_confidence:
            ax2.set_xlim(west_3857, east_3857)
            ax2.set_ylim(south_3857, north_3857)
            
            # Add base map
            try:
                ctx.add_basemap(ax2, crs='EPSG:3857', source=map_source, alpha=0.8)
            except Exception as e:
                print(f"Warning: Could not load base map for confidence plot: {e}")
            
            # Mask invalid confidences
            confidences_masked = np.ma.masked_where(np.isnan(confidences_2d), confidences_2d)
            
            # Overlay confidence
            im2 = ax2.imshow(confidences_masked, extent=extent_3857, origin='lower',
                            cmap='plasma', alpha=alpha, vmin=0, vmax=1)
            
            ax2.set_title('Prediction Confidence on Map', fontsize=14, pad=20)
            ax2.set_xlabel('Longitude', fontsize=12)
            ax2.set_ylabel('Latitude', fontsize=12)
            
            # Add colorbar for confidence
            cbar2 = plt.colorbar(im2, ax=ax2, shrink=0.8, pad=0.02)
            cbar2.set_label('Confidence', fontsize=11)
        
        # Add coordinate information to the title
        fig.suptitle(f'Map Classification Results\n'
                    f'Area: {min_lat:.4f}° to {max_lat:.4f}°N, {min_lon:.4f}° to {max_lon:.4f}°E',
                    fontsize=16, y=0.95)
        
        plt.tight_layout()
        
        if save_path:
            plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
            print(f"Map overlay visualization saved to {save_path}")
        
        plt.show()

    def create_interactive_map(self, predictions_2d, confidences_2d, bbox, grid_points, 
                            output_path='classification_map.html'):
        """
        Create an interactive map with classification results using Folium
        
        Args:
            predictions_2d (np.array): 2D array of predictions
            confidences_2d (np.array): 2D array of confidences  
            bbox (list): Bounding box [min_lon, min_lat, max_lon, max_lat]
            grid_points (list): Original grid points
            output_path (str): Path to save the HTML map
        """
        try:
            import folium
            from folium import plugins
        except ImportError:
            print("Error: folium is required for interactive maps.")
            print("Install it with: pip install folium")
            return
        
        min_lon, min_lat, max_lon, max_lat = bbox
        
        # Calculate center point
        center_lat = (min_lat + max_lat) / 2
        center_lon = (min_lon + max_lon) / 2
        
        # Create base map
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=10,
                # tiles='OpenStreetMap',  # Standard with all labels
                # tiles='CartoDB Voyager',  # Clean with labels
                # tiles='Esri.WorldTopoMap',  # Topographic with labels
                tiles='https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}',
                attr='Esri'
        )
        
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
            attr='Esri',
            name='Labels',
            overlay=True,
            control=False
        ).add_to(m)

        # Add bounding box rectangle
        folium.Rectangle(
            bounds=[[min_lat, min_lon], [max_lat, max_lon]],
            color='red',
            weight=2,
            fill=False,
            popup=f'Classification Area<br>Bounds: {min_lat:.4f}°-{max_lat:.4f}°N<br>{min_lon:.4f}°-{max_lon:.4f}°E'
        ).add_to(m)
        
        # Create color map for classes
        colors = ['blue', 'gray']
        
        # Add grid cells as rectangles with classification results
        n_rows, n_cols = predictions_2d.shape
        cell_height = (max_lat - min_lat) / n_rows
        cell_width = (max_lon - min_lon) / n_cols
        
        for point in grid_points:
            row, col = point['row'], point['col']
            
            # Skip if no prediction available
            if predictions_2d[row, col] == -1:
                continue
                
            prediction = predictions_2d[row, col]
            confidence = confidences_2d[row, col]
            
            # Calculate cell bounds
            cell_min_lat = min_lat + row * cell_height
            cell_max_lat = cell_min_lat + cell_height
            cell_min_lon = min_lon + col * cell_width
            cell_max_lon = cell_min_lon + cell_width
            
            # Choose color based on prediction
            color = colors[prediction % len(colors)]
            if prediction == 0 and confidence < 0.7:
                color = 'lightblue'
            if prediction == 0 and confidence >= 0.7 and confidence < 0.9:
                color = 'blue'
            if prediction == 0 and confidence >= 0.9:
                color = 'darkblue'
            
            # Create popup text
            popup_text = f"""
            <b>Grid Cell ({row}, {col})</b><br>
            <b>Predicted Class:</b> {self.class_names[prediction]}<br>
            <b>Confidence:</b> {confidence:.3f}<br>
            <b>Coordinates:</b><br>
            Lat: {point['latitude']:.4f}°<br>
            Lon: {point['longitude']:.4f}°
            """
            
            # Add rectangle for this grid cell
            folium.Rectangle(
                bounds=[[cell_min_lat, cell_min_lon], [cell_max_lat, cell_max_lon]],
                color=color,
                weight=0.1,
                fillColor=color,
                fillOpacity=max(0.2, 0.4),
                popup=folium.Popup(popup_text, max_width=300),
                tooltip=f"Class: {self.class_names[prediction]} (conf: {confidence:.2f})"
            ).add_to(m)
        
        # Add legend
        legend_html = '''
        <div style="position: fixed; 
                    bottom: 50px; right: 50px; width: 200px; height: auto; 
                    background-color: white; border:2px solid grey; z-index:9999; 
                    font-size:14px; padding: 10px;
                    ">
        <h4>Classification Legend</h4>
        '''
        
        # for i, class_name in enumerate(self.class_names):
        #     color = colors[i % len(colors)]
        legend_html += f'<p><i class="fa fa-square" style="color:lightblue"></i> Hydro Potential (<70% confidence)</p>'
        legend_html += f'<p><i class="fa fa-square" style="color:blue"></i> Hydro Potential (70% - 90% confidence)</p>'
        legend_html += f'<p><i class="fa fa-square" style="color:darkblue"></i> Hydro Potential (>90% confidence)</p>'
        legend_html += f'<p><i class="fa fa-square" style="color:grey"></i> No Hydro</p>'
        
        legend_html += '</div>'
        m.get_root().html.add_child(folium.Element(legend_html))
        
        # Add fullscreen button
        plugins.Fullscreen().add_to(m)
        
        # Save map
        m.save(output_path)
        print(f"Interactive map saved to {output_path}")
        
        return m


def classify_map_area(bbox, model_path, model_name='resnet34', class_names=None, 
                     output_folder='classification_results', temp_folder='temp_grid_data'):
    """
    Main function to classify a map area
    
    Args:
        bbox (list): [min_lon, min_lat, max_lon, max_lat]
        model_path (str): Path to trained model
        model_name (str): Model architecture
        class_names (list): Class names for visualization
        output_folder (str): Output folder for results
        temp_folder (str): Temporary folder for feature extraction
        max_workers (int): Number of parallel workers
        cleanup (bool): Whether to cleanup temporary files
        
    Returns:
        tuple: (predictions_2d, confidences_2d, results_summary)
    """
    print("=" * 60)
    print("MAP AREA CLASSIFICATION")
    print("=" * 60)
    print(f"Bounding box: {bbox}")
    print(f"Model: {model_name} from {model_path}")
    print(f"Output folder: {output_folder}")
    print()
    
    classifier = MapClassifier(model_path, model_name, 2, class_names)
    
    print("Step 1: Creating grid...")
    grid_points, grid_shape, cell_size = classifier.create_grid(bbox)
    print(f"Created grid with {len(grid_points)} cells")
    print()
    
    print("Step 2: Extracting satellite features...")
    feature_files = classifier.extract_features_for_grid(
        grid_points, temp_folder
    )
    print()
    
    print("Step 3: Running classification...")
    predictions_2d, confidences_2d, results_list = classifier.classify_grid(
        feature_files, grid_shape
    )
    print()
    
    valid_predictions = predictions_2d[predictions_2d >= 0]
    if len(valid_predictions) > 0:
        class_counts = np.bincount(valid_predictions)
        print("Step 4: Classification Summary")
        for i, count in enumerate(class_counts):
            class_name = classifier.class_names[i] if i < len(classifier.class_names) else f'Class_{i}'
            percentage = count / len(valid_predictions) * 100
            print(f"  {class_name}: {count} cells ({percentage:.1f}%)")
        
        avg_confidence = np.nanmean(confidences_2d)
        print(f"  Average confidence: {avg_confidence:.3f}")
    print()
    
    print("Step 5: Saving results...")
    classifier.visualize_results(predictions_2d, confidences_2d, bbox, 
                               f"{output_folder}/classification_map.png")
    
        # Create overlay visualization on real map
    print("Creating real map overlay...")
    classifier.create_interactive_map(predictions_2d, confidences_2d, bbox, grid_points, output_folder + "/interactive_map.html")
    
    result_folder = classifier.save_results(
        predictions_2d, confidences_2d, bbox, grid_points, output_folder
    )
    
    # Summary - convert numpy types for JSON compatibility
    results_summary = {
        'grid_shape': [int(dim) for dim in grid_shape],  # Convert to Python int
        'total_cells': int(len(grid_points)),
        'classified_cells': int(len(feature_files)),
        'class_distribution': {name: int(count) for name, count in zip(classifier.class_names, 
                             np.bincount(valid_predictions))} if len(valid_predictions) > 0 else {},
        'average_confidence': float(np.nanmean(confidences_2d)) if not np.isnan(np.nanmean(confidences_2d)) else 0.0,
        'output_folder': str(result_folder)
    }
    
    print("=" * 60)
    print("CLASSIFICATION COMPLETE!")
    print(f"Results saved to: {result_folder}")
    print("=" * 60)
    
    return predictions_2d, confidences_2d, results_summary


# bbox = [24.22, 44.95, 25.10, 45.31]  # [min_lon, min_lat, max_lon, max_lat]
# model_path = "cnn_hydro.pth"
# class_names = ['Hydro Potential', 'No Hydro']

# predictions, confidences, summary = classify_map_area(
#     bbox=bbox,
#     model_path=model_path,
#     model_name='resnet34',
#     class_names=class_names,
#     output_folder='data/map',
# )

# print("Classification Summary:")
# for key, value in summary.items():
#     print(f"  {key}: {value}")